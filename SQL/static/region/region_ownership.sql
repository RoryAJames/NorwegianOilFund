SELECT * 
FROM
(SELECT year, region AS "Region", ROUND(AVG(percent_ownership), 2) AS avg_percent_ownership
FROM oil_fund
WHERE category = 'Equity' 
GROUP BY year, region
ORDER BY year) as subq
WHERE avg_percent_ownership >= 0;