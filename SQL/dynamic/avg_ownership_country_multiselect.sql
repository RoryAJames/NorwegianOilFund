SELECT year, country AS "Country", ROUND(AVG(percent_ownership), 2) AS avg_percent_ownership
FROM oil_fund
WHERE category = 'Equity' AND year > (SELECT MAX(year) - (%s + 1) FROM oil_fund)
AND country IN ({}) 
GROUP BY year, country
ORDER BY year