CREATE TABLE gads_dim_next.ad (
  ad_id            BIGINT NOT NULL,
  ad_name          TEXT   NOT NULL,
  ad_group_name    TEXT   NOT NULL,
  campaign_name    TEXT   NOT NULL,
  account_name     TEXT   NOT NULL,
  account_currency TEXT   NOT NULL,
  _attributes      JSONB  NOT NULL
);

INSERT INTO gads_dim_next.ad
  SELECT
    ad_id,
    coalesce(ad_name, ad_id :: VARCHAR),
    ad_group_name,
    campaign_name,
    account_name,
    currency,
    attributes

  FROM gads_data.campaign_structure
  WHERE ad_id IN (SELECT DISTINCT ad_id
                  FROM gads_data.ad_performance);

SELECT util.add_pk('gads_dim_next', 'ad');

ANALYZE gads_dim_next.ad;
