CREATE TABLE gads_dim_next.network_type (
  network_type_id   SMALLSERIAL PRIMARY KEY,
  network_type_name TEXT NOT NULL UNIQUE
);

INSERT INTO gads_dim_next.network_type (network_type_name)
  SELECT DISTINCT network
  FROM gads_data.ad_performance
  ORDER BY network;

ANALYZE gads_dim_next.network_type;

CREATE TABLE gads_dim_next.device (
  device_id   SMALLSERIAL PRIMARY KEY,
  device_name TEXT NOT NULL UNIQUE
);

INSERT INTO gads_dim_next.device (device_name)
  SELECT DISTINCT device
  FROM gads_data.ad_performance
  ORDER BY device;

ANALYZE gads_dim_next.device;


CREATE TABLE gads_dim_next.ad_performance (
  day_fk                           BIGINT   NOT NULL,
  ad_fk                            BIGINT   NOT NULL,

  network_type_fk                  SMALLINT NOT NULL,
  device_fk                        SMALLINT NOT NULL,

  active_view_viewable_impressions INTEGER,
  clicks                           REAL,
  conversions                      INTEGER,
  conversion_value                 REAL,
  cost                             DOUBLE PRECISION,
  impressions                      INTEGER,
  summed_position                  REAL,
  average_position                 REAL,

  _date                            DATE     NOT NULL
);

INSERT INTO gads_dim_next.ad_performance
  SELECT
    to_char(ad_performance.date, 'YYYYMMDD') :: BIGINT              AS day_fk,
    ad_performance.ad_id                                            AS ad_fk,

    network_type_id                                                 AS network_type_fk,
    device_id                                                       AS device_fk,

    nullif(active_view_viewable_impressions, 0),
    nullif(clicks, 0),
    nullif(conversions, 0),
    nullif(conversion_value / exchange_rate, 0),
    nullif(COST :: DOUBLE PRECISION / 1000000.0 / exchange_rate, 0) AS cost,
    nullif(impressions, 0),
    nullif(average_position * impressions, 0)                       AS summed_position,
    nullif(average_position, 0)                                     AS average_position,
    ad_performance.date
  FROM gads_data.ad_performance
    JOIN gads_data.campaign_structure USING (ad_id)
    LEFT JOIN euro_fx.exchange_rate
      ON exchange_rate.currency = campaign_structure.currency
         AND exchange_rate.date = ad_performance.date
    LEFT JOIN gads_dim_next.network_type ON network = network_type_name
    LEFT JOIN gads_dim_next.device ON device = device_name;

CREATE FUNCTION gads_tmp.constrain_ad_performance()
  RETURNS VOID AS $$

SELECT util.add_fk('gads_dim_next', 'ad_performance', 'time', 'day');
SELECT util.add_fk('gads_dim_next', 'ad_performance', 'gads_dim_next', 'ad');
SELECT util.add_fk('gads_dim_next', 'ad_performance', 'gads_dim_next', 'network_type');
SELECT util.add_fk('gads_dim_next', 'ad_performance', 'gads_dim_next', 'device');

$$
LANGUAGE SQL;
