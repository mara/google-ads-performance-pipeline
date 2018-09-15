import pathlib
from google_ads_performance_pipeline import config

from data_integration.commands.files import ReadFile, Compression
from data_integration.commands.sql import ExecuteSQL
from data_integration.parallel_tasks.files import ParallelReadFile, ReadMode
from data_integration.parallel_tasks.sql import ParallelExecuteSQL
from data_integration.pipelines import Pipeline, Task

pipeline = Pipeline(id="google_ads", description="Processes the data downloaded from the Google Ads API",
                    base_path=pathlib.Path(__file__).parent, labels={"Schema": "gads_dim"})

pipeline.add_initial(
    Task(id="initialize_schemas",
         description="Recreates the tmp and data schemas",
         commands=[
             # create data schema only once (or when create_data_schema.sql changes)
             ExecuteSQL(sql_file_name="create_data_schema.sql", echo_queries=False,
                        file_dependencies=['create_data_schema.sql']),
             ExecuteSQL(sql_file_name="recreate_schemas.sql", echo_queries=True)
         ]))

pipeline.add(
    Task(id="read_campaign_structure",
         description="Loads the google ads campaign structure",
         commands=[
             ExecuteSQL(sql_file_name="create_campaign_structure_data_table.sql", echo_queries=False),
             ReadFile(file_name="google-ads-account-structure_{}.csv.gz".format(config.input_file_version()),
                      compression=Compression.GZIP, skip_header=True,
                      target_table="gads_data.campaign_structure",
                      delimiter_char="\t", null_value_string="", csv_format=True)
         ]))

pipeline.add(
    ParallelReadFile(
        id="read_ad_performance",
        description="Loads ad performance data from json files",
        file_pattern="*/*/*/google-ads/ad-performance_{}.json.gz".format(config.input_file_version()),
        read_mode=config.read_mode(),
        compression=Compression.GZIP, make_unique=True,
        mapper_script_file_name="read_ad_performance.py",
        target_table="gads_data.ad_performance_upsert",
        delimiter_char="\t",
        date_regex="^(?P<year>\d{4})\/(?P<month>\d{2})\/(?P<day>\d{2})/",
        file_dependencies=['create_ad_performance_data_table.sql'],
        commands_before=[
            ExecuteSQL(sql_file_name="create_ad_performance_data_table.sql", echo_queries=False,
                       file_dependencies=['create_ad_performance_data_table.sql'])
        ],
        commands_after=[
            ExecuteSQL(sql_statement='SELECT gads_data.upsert_ad_performance()')
        ]))

pipeline.add(
    Task(id="transform_ad",
         description="Creates the ad dimension table",
         commands=[
             ExecuteSQL(sql_file_name="transform_ad.sql")
         ]),
    upstreams=["read_campaign_structure", "read_ad_performance"])

pipeline.add(
    ParallelExecuteSQL(
        id="index_ad",
        description="Adds indexes to all name columns of the ad dimension",
        sql_statement="SELECT util.add_index('gads_dim_next', 'ad', column_names := ARRAY ['@column@']);",
        parameter_function=lambda: [('ad_name',), ('ad_group_name',), ('campaign_name',), ('account_name',)],
        parameter_placeholders=["@column@"]),
    upstreams=["transform_ad"])

pipeline.add(
    Task(id="transform_ad_performance",
         description="Creates the fact table of the google ads performance cube",
         commands=[
             ExecuteSQL(sql_file_name="transform-ad-performance.sql")
         ]),
    upstreams=["read_ad_performance", "read_campaign_structure"])

pipeline.add(
    ParallelExecuteSQL(
        id="index_ad_performance",
        description="Adds indexes to all fk columns of the ad performance fact table",
        sql_statement="SELECT util.add_index('gads_dim_next', 'ad_performance',column_names := ARRAY ['@column@']);",
        parameter_function=lambda: [('day_fk',), ('ad_fk',), ('network_type_fk',), ('device_fk',)],
        parameter_placeholders=["@column@"]),
    upstreams=["transform_ad_performance"])

pipeline.add(
    Task(id="transform_ad_attribute",
         description="Creates the ad_attribute and ad_attribute_mapping dimension tables",
         commands=[
             ExecuteSQL(sql_file_name="transform_ad_attribute.sql")
         ]),
    upstreams=["transform_ad"])

pipeline.add_final(
    Task(id="replace_dim_schema",
         description="Replaces the current dim schema with the contents of dim_next",
         commands=[
             ExecuteSQL(sql_statement="SELECT gads_tmp.constrain_ad_performance();"),
             ExecuteSQL(sql_statement="SELECT gads_tmp.constrain_ad_attribute_mapping();"),
             ExecuteSQL(sql_statement="SELECT util.replace_schema('gads_dim', 'gads_dim_next');")
         ]))
