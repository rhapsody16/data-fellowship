SELECT
  COUNT(totals_transactions) AS totals_transactions_date,
  date
FROM
  `data-fellowship-8.df8.rev_transactions_deduplicated`
GROUP BY
  date
ORDER BY
  date ASC;