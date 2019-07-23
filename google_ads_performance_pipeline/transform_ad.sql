CREATE TABLE gads_dim_next.ad (
  ad_id            INTEGER NOT NULL,
  gads_ad_id       BIGINT  NOT NULL,
  ad_name          TEXT    NOT NULL,
  ad_group_id      BIGINT  NOT NULL,
  ad_group_name    TEXT    NOT NULL,
  campaign_name    TEXT    NOT NULL,
  account_name     TEXT    NOT NULL,
  account_currency TEXT    NOT NULL,
  _attributes      JSONB   NOT NULL
);

INSERT INTO gads_dim_next.ad
SELECT DISTINCT
          dense_rank()
          OVER (
            PARTITION BY 1
            ORDER BY ad_id, ad_group_id)      AS ad_id,
          ad_id                               AS gads_ad_id,
          coalesce(ad_name, ad_id :: VARCHAR) AS ad_name,
          ad_group_id,
          ad_group_name,
          campaign_name,
          account_name,
          currency,
          attributes

  FROM gads_data.ad_campaign_structure
  WHERE (ad_id, ad_group_id) IN (SELECT DISTINCT
                    ad_id, ad_group_id
                    FROM gads_data.ad_performance);

SELECT util.add_pk('gads_dim_next', 'ad');

ANALYZE gads_dim_next.ad;
