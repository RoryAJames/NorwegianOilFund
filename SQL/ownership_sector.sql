SELECT year, sector AS "Sector", ROUND(AVG(percent_ownership), 2) AS avg_percent_ownership
FROM oil_fund
WHERE category = 'Equity'
GROUP BY year, sector
ORDER BY year;