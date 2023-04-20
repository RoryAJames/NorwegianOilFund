SELECT year as Year,
ROUND(SUM(market_value) FILTER (WHERE category = 'Equity')/SUM(market_value) * 100.00,2) AS "Equity Proportion",
ROUND(SUM(market_value) FILTER (WHERE category = 'Fixed Income')/SUM(market_value) *100.00,2) AS "Fixed Income Proportion"
FROM oil_fund
GROUP BY year;