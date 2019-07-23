CREATE TABLE gads_dim_next.keyword_performance (
  day_fk                           INTEGER  NOT NULL,
  keyword_fk                       INTEGER  NOT NULL,

  network_type_fk                  SMALLINT NOT NULL,
  device_fk                        SMALLINT NOT NULL,

  active_view_viewable_impressions INTEGER,
  clicks                           REAL,
  conversions                      INTEGER,
  conversion_value                 REAL,
  cost                             DOUBLE PRECISION,
  impressions                      INTEGER,
  summed_position                  REAL,
  average_position                 REAL
);

INSERT INTO gads_dim_next.keyword_performance
SELECT
  to_char(kp.keyword_date, 'YYYYMMDD') :: INTEGER                 AS day_fk,
  kw.keyword_id                                                   AS keyword_fk,

  nt.network_type_id                                              AS network_type_fk,
  dev.device_id                                                   AS device_fk,

  nullif(active_view_viewable_impressions, 0),
  nullif(clicks, 0),
  nullif(conversions, 0),
  nullif(conversion_value / exchange_rate, 0),
  nullif(cost :: DOUBLE PRECISION / 1000000.0 / exchange_rate, 0) AS cost,
  nullif(impressions, 0),
  nullif(average_position * impressions, 0)                       AS summed_position,
  nullif(average_position, 0)                                     AS average_position
  FROM gads_data.keyword_performance kp
  JOIN gads_dim_next.keyword kw ON kp.keyword_id = kw.gads_keyword_id AND kp.ad_group_id = kw.ad_group_id
  LEFT JOIN euro_fx.exchange_rate
            ON exchange_rate.currency = kw.account_currency
              AND exchange_rate.date = kp.keyword_date
  LEFT JOIN gads_dim_next.network_type nt ON kp.network = nt.network_type_name
  LEFT JOIN gads_dim_next.device dev ON kp.device = dev.device_name;

CREATE FUNCTION gads_tmp.constrain_keyword_performance()
  RETURNS VOID AS
$$

SELECT
  util.add_fk('gads_dim_next', 'keyword_performance', 'time', 'day');
SELECT
  util.add_fk('gads_dim_next', 'keyword_performance', 'gads_dim_next', 'keyword');
SELECT
  util.add_fk('gads_dim_next', 'keyword_performance', 'gads_dim_next', 'network_type');
SELECT
  util.add_fk('gads_dim_next', 'keyword_performance', 'gads_dim_next', 'device');

$$
  LANGUAGE SQL;
