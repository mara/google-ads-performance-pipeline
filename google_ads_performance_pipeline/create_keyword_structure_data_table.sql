DROP TABLE IF EXISTS gads_data.keyword_structure;

CREATE TABLE gads_data.keyword_structure (
  keyword_id    BIGINT NOT NULL,
  keyword_name  VARCHAR,
  ad_group_id   BIGINT NOT NULL,
  ad_group_name TEXT   NOT NULL,
  campaign_id   BIGINT NOT NULL,
  campaign_name TEXT   NOT NULL,
  account_id    BIGINT NOT NULL,
  account_name  TEXT   NOT NULL,
  attributes    JSONB  NOT NULL,
  currency      TEXT
);
