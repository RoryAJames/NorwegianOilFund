WITH yearly_avg AS (
    SELECT year, sector, AVG(percent_ownership) AS avg_percent_ownership
    FROM oil_fund
    WHERE category = 'Equity' AND year > (SELECT MAX(year) - (%s + 1) FROM oil_fund)
    GROUP BY year, sector
    ORDER BY year, sector
),
difference AS (
    SELECT year, sector, 
    avg_percent_ownership - LAG(avg_percent_ownership) OVER (PARTITION BY sector ORDER BY year) AS difference
    FROM yearly_avg
),
running_total AS (
    SELECT year, sector, 
    ROUND(SUM(difference) OVER (PARTITION BY sector ORDER BY year), 2) AS cumulative_bp_change_of_ownership
    FROM difference
    WHERE difference IS NOT NULL
)

SELECT sector AS "Sector", cumulative_bp_change_of_ownership
FROM running_total
WHERE year = (SELECT MAX(year)FROM oil_fund)
ORDER BY cumulative_bp_change_of_ownership DESC;