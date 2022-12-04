WITH
  transaction_per_date AS (
  SELECT
    channelGrouping,
    PARSE_DATE("%Y%m%d", date) AS date_parsed,
    SUM(totals_transactions) AS trx_per_date
  FROM
    `data-fellowship-8.df8.rev_transactions_deduplicated`
  GROUP BY
    channelGrouping,
    date_parsed ),
  transaction_per_country AS (
  SELECT
    channelGrouping,
    geoNetwork_country,
    PARSE_DATE("%Y%m%d", date) AS date_parsed,
    SUM(totals_transactions) AS transactions_per_country
  FROM
    `data-to-insights.ecommerce.rev_transactions`
  WHERE
    geoNetwork_country != "(not set)"
    AND channelGrouping != "(Other)"
  GROUP BY
    channelGrouping,
    geoNetwork_country,
    date_parsed )
SELECT
  trxdate.channelGrouping,
  trxdate.date_parsed,
  trx_per_date,
  ARRAY_AGG( STRUCT(geoNetwork_country,
      transactions_per_country) ) AS aggregated_country,
FROM
  transaction_per_date AS trxdate
INNER JOIN
  transaction_per_country AS trxcountry
ON
  trxdate.channelGrouping = trxcountry.channelGrouping
  AND trxdate.date_parsed = trxcountry.date_parsed
WHERE
  geoNetwork_country != "(not set)"
  AND trxdate.channelGrouping != "(Other)"
GROUP BY
  channelGrouping,
  date_parsed,
  trx_per_date
ORDER BY
  date_parsed;