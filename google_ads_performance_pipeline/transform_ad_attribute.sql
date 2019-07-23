DROP TABLE IF EXISTS gads_dim_next.ad_attribute_value CASCADE;

CREATE TABLE gads_dim_next.ad_attribute_value (
  ad_attribute_value_id   SERIAL PRIMARY KEY,
  ad_attribute_value_name TEXT     NOT NULL,
  ad_attribute_id         SMALLINT NOT NULL,
  ad_attribute_name       TEXT     NOT NULL
);


INSERT INTO gads_dim_next.ad_attribute_value

WITH attributes AS (
  SELECT DISTINCT
    (jsonb_each_text(_attributes)).*
    FROM gads_dim_next.ad),
     attributes_with_ids AS (
       SELECT
                 row_number()
                 OVER (
                   ORDER BY key, value ) AS value_id,

                 value,
                 key                     AS attribute
         FROM attributes)

SELECT
  value_id,
  value,
  first_value(value_id)
  OVER (
    PARTITION BY attribute ) AS attribute_id,
  attribute
  FROM attributes_with_ids
  ORDER BY value_id;

DROP TABLE IF EXISTS gads_dim_next.ad_attribute_mapping CASCADE;

CREATE TABLE gads_dim_next.ad_attribute_mapping (
  ad_fk                 BIGINT   NOT NULL,
  ad_attribute_value_fk SMALLINT NOT NULL
);

INSERT INTO gads_dim_next.ad_attribute_mapping
SELECT
  ad_id,
  ad_attribute_value_id
  FROM (SELECT
          ad_id,

          (jsonb_each_text(_attributes)).*
          FROM gads_dim_next.ad) t
  JOIN gads_dim_next.ad_attribute_value
       ON ad_attribute_value_name = value AND ad_attribute_name = key
  ORDER BY ad_id;

-- make values unique
WITH duplicate AS
       (SELECT *
          FROM (SELECT
                  ad_attribute_value_id,
                  count(*)
                  OVER (
                    PARTITION BY ad_attribute_value_name ) AS n,
                  dense_rank()
                  OVER (
                    PARTITION BY ad_attribute_value_name
                    ORDER BY ad_attribute_value_id )       AS rank
                  FROM gads_dim_next.ad_attribute_value) t
          WHERE n > 1)

UPDATE gads_dim_next.ad_attribute_value v
SET ad_attribute_value_name = ad_attribute_value_name || ' (' || rank || ')'
  FROM duplicate
  WHERE duplicate.ad_attribute_value_id = v.ad_attribute_value_id;

-- allow to use the ad attribute dimension multiple times
CREATE FUNCTION gads_tmp.create_ad_attribute_mapping_views()
  RETURNS VOID AS
$$
DECLARE
  i INTEGER := 1;
BEGIN
  WHILE i <= 10
    LOOP
      EXECUTE 'CREATE VIEW gads_dim_next.ad_attribute_mapping_' || i ||
              ' AS (SELECT * FROM gads_dim_next.ad_attribute_mapping);';
      i := i + 1;
    END LOOP;
END;
$$
  LANGUAGE plpgsql;

SELECT gads_tmp.create_ad_attribute_mapping_views();


CREATE FUNCTION gads_tmp.constrain_ad_attribute_mapping()
  RETURNS VOID AS
$$
SELECT
  util.add_fk('gads_dim_next', 'ad_attribute_mapping', 'gads_dim_next', 'ad');
SELECT
  util.add_fk('gads_dim_next', 'ad_attribute_mapping', 'gads_dim_next', 'ad_attribute_value');
SELECT
  util.add_index('gads_dim_next', 'ad_attribute_mapping',
                 column_names := ARRAY ['ad_attribute_value_fk']);

$$
  LANGUAGE SQL;
