CREATE TABLE gads_dim_next.keyword (
  keyword_id       INTEGER NOT NULL,
  gads_keyword_id  BIGINT  NOT NULL,
  keyword_name     TEXT    NOT NULL,
  ad_group_id      BIGINT  NOT NULL,
  ad_group_name    TEXT    NOT NULL,
  campaign_name    TEXT    NOT NULL,
  account_name     TEXT    NOT NULL,
  account_currency TEXT    NOT NULL,
  _attributes      JSONB   NOT NULL
);

INSERT INTO gads_dim_next.keyword
SELECT DISTINCT
          dense_rank()
          OVER (
            PARTITION BY 1
            ORDER BY keyword_id, ad_group_id) AS keyword_id,
          keyword_id                          AS gads_keyword_id,
          keyword_name,
          ad_group_id,
          ad_group_name,
          campaign_name,
          account_name,
          currency,
          attributes

  FROM gads_data.keyword_structure
  WHERE (keyword_id, ad_group_id) IN (SELECT DISTINCT
                                        keyword_id,
                                        ad_group_id
                                        FROM gads_data.keyword_performance);

SELECT util.add_pk('gads_dim_next', 'keyword');

ANALYZE gads_dim_next.keyword;
