SELECT
  COUNT(totals_transactions) AS totals_transactions_country,
  geoNetwork_country
FROM
  `data-fellowship-8.df8.rev_transactions_deduplicated`
GROUP BY
  geoNetwork_country
ORDER BY
  geoNetwork_country ASC;