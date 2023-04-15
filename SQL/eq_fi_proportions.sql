SELECT year,
ROUND((SUM(CASE WHEN category = 'Equity' THEN market_value ELSE 0 END) / NULLIF(SUM(market_value),0)) * 100, 2) AS "Equity Percentage",
ROUND((SUM(CASE WHEN category = 'Fixed Income' THEN market_value ELSE 0 END) / NULLIF(SUM(market_value),0)) * 100, 2) AS "Fixed Income Percentage"
FROM oil_fund
GROUP BY year;