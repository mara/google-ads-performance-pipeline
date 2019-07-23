CREATE TABLE gads_dim_next.ad_performance (
  day_fk                           INTEGER  NOT NULL,
  ad_fk                            INTEGER  NOT NULL,

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

INSERT INTO gads_dim_next.ad_performance
SELECT
  to_char(ap.ad_date, 'YYYYMMDD') :: INTEGER                      AS day_fk,
  ad.ad_id                                                        AS ad_fk,

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
  FROM gads_data.ad_performance ap
  JOIN gads_dim_next.ad ad ON ap.ad_id = ad.gads_ad_id AND ap.ad_group_id = ad.ad_group_id
  LEFT JOIN euro_fx.exchange_rate
            ON exchange_rate.currency = ad.account_currency
              AND exchange_rate.date = ap.ad_date
  LEFT JOIN gads_dim_next.network_type nt ON ap.network = nt.network_type_name
  LEFT JOIN gads_dim_next.device dev ON ap.device = dev.device_name;

CREATE FUNCTION gads_tmp.constrain_ad_performance()
  RETURNS VOID AS
$$

SELECT util.add_fk('gads_dim_next', 'ad_performance', 'time', 'day');
SELECT
  util.add_fk('gads_dim_next', 'ad_performance', 'gads_dim_next', 'ad');
SELECT
  util.add_fk('gads_dim_next', 'ad_performance', 'gads_dim_next', 'network_type');
SELECT
  util.add_fk('gads_dim_next', 'ad_performance', 'gads_dim_next', 'device');

$$
  LANGUAGE SQL;
