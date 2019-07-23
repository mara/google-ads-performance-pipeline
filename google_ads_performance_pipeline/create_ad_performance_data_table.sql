DROP TABLE IF EXISTS gads_data.ad_performance CASCADE;

-- the persistent data table
CREATE TABLE gads_data.ad_performance (
  ad_date                          DATE    NOT NULL,
  ad_id                            BIGINT  NOT NULL,
  ad_group_id                      BIGINT  NOT NULL,

  device                           TEXT    NOT NULL,
  network                          TEXT    NOT NULL,

  active_view_viewable_impressions INTEGER NOT NULL,
  average_position                 REAL    NOT NULL,
  clicks                           REAL    NOT NULL,
  conversions                      REAL    NOT NULL,
  conversion_value                 REAL    NOT NULL,
  cost                             BIGINT  NOT NULL,
  impressions                      INTEGER NOT NULL
);

-- needed for upserting
SELECT
  util.add_index('gads_data', 'ad_performance', column_names := ARRAY ['ad_date', 'network']);

-- create an exact copy of the data table. New data will be copied here
DROP TABLE IF EXISTS gads_data.ad_performance_upsert;

CREATE TABLE gads_data.ad_performance_upsert AS
  SELECT *
    FROM gads_data.ad_performance
    LIMIT 0;


CREATE OR REPLACE FUNCTION gads_data.upsert_ad_performance()
  RETURNS VOID AS
'

  -- rather than doing a proper upsert, first data for the dates and (ad_id, ad_group_id) in the upsert table
  DELETE
    FROM gads_data.ad_performance USING gads_data.ad_performance_upsert
    WHERE ad_performance_upsert.ad_date = ad_performance.ad_date
      AND ad_performance_upsert.ad_id = ad_performance.ad_id
      AND ad_performance_upsert.ad_group_id = ad_performance.ad_group_id;

  -- copy new data in
  INSERT INTO gads_data.ad_performance
  SELECT *
    FROM gads_data.ad_performance_upsert;

  -- remove tmp data
  TRUNCATE gads_data.ad_performance_upsert;

'
  LANGUAGE SQL;
