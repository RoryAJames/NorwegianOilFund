SELECT year, industry, ROUND(AVG(percent_ownership)::numeric, 2) AS avg_percent_ownership
FROM oil_fund
WHERE category = 'Equity'
GROUP BY year, industry
ORDER BY year;