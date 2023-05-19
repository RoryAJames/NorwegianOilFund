WITH yearly_avg AS (
    SELECT year, name, sector, AVG(percent_ownership) AS avg_percent_ownership
    FROM oil_fund
    WHERE category = 'Equity' AND year > (SELECT MAX(year) - (%s + 1) FROM oil_fund)
    GROUP BY year, name, sector
    ORDER BY year, name, sector
),
difference AS (
    SELECT year, name, sector, 
           avg_percent_ownership - LAG(avg_percent_ownership) OVER (PARTITION BY name ORDER BY year) AS difference
    FROM yearly_avg
),
running_total AS (
    SELECT year, name, sector, 
           ROUND(SUM(difference) OVER (PARTITION BY name ORDER BY year), 2) AS cumulative_bp_change_of_ownership
    FROM difference
    WHERE difference IS NOT NULL
)

SELECT name AS "Company", sector as "Sector", cumulative_bp_change_of_ownership
FROM running_total
WHERE year = (SELECT MAX(year)FROM oil_fund)
ORDER BY cumulative_bp_change_of_ownership DESC
LIMIT 10;