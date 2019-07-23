CREATE TABLE gads_dim_next.network_type (
  network_type_id   SMALLSERIAL PRIMARY KEY,
  network_type_name TEXT NOT NULL UNIQUE
);

INSERT INTO gads_dim_next.network_type (network_type_name)
SELECT DISTINCT
  network
  FROM gads_data.ad_performance
  ORDER BY network;

INSERT INTO gads_dim_next.network_type (network_type_name)
SELECT DISTINCT
  network
  FROM gads_data.keyword_performance
  WHERE network NOT IN (
    SELECT
      network_type_name
      FROM gads_dim_next.network_type
  )
  ORDER BY network;


ANALYZE gads_dim_next.network_type;
