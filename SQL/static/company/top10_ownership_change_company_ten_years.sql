WITH yearly_avg AS (
    SELECT year, name, AVG(percent_ownership) AS avg_percent_ownership
    FROM oil_fund
    WHERE category = 'Equity' AND year > (SELECT MAX(year) - 11 FROM oil_fund)
    GROUP BY year, name 
    ORDER BY year, name 
),
difference AS (
    SELECT year, name, 
           avg_percent_ownership - LAG(avg_percent_ownership) OVER (PARTITION BY name ORDER BY year) AS difference
    FROM yearly_avg
),
running_total AS (
    SELECT year, name, 
           ROUND(SUM(difference) OVER (PARTITION BY name ORDER BY year), 2) AS cumulative_bp_change_of_ownership
    FROM difference
    WHERE difference IS NOT NULL
)

SELECT name AS "Company", cumulative_bp_change_of_ownership
FROM running_total
WHERE year = (SELECT MAX(year)FROM oil_fund)
ORDER BY cumulative_bp_change_of_ownership DESC
LIMIT 10;