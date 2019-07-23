CREATE TABLE gads_dim_next.device (
  device_id   SMALLSERIAL PRIMARY KEY,
  device_name TEXT NOT NULL UNIQUE
);

INSERT INTO gads_dim_next.device (device_name)
SELECT DISTINCT
  device
  FROM gads_data.ad_performance
  ORDER BY device;

INSERT INTO gads_dim_next.device (device_name)
SELECT DISTINCT
  device
  FROM gads_data.keyword_performance
  WHERE device NOT IN (
    SELECT
      device_name
      FROM gads_dim_next.device
  )
  ORDER BY device;

ANALYZE gads_dim_next.device;
