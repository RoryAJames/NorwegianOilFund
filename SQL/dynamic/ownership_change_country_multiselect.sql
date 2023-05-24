WITH yearly_avg AS (
    SELECT year, country, AVG(percent_ownership) AS avg_percent_ownership
    FROM oil_fund
    WHERE category = 'Equity' AND year > (SELECT MAX(year) - (%s + 1) FROM oil_fund)
    AND country IN ({})
    GROUP BY year, country
    ORDER BY year, country
),
difference AS (
    SELECT year, country, 
    avg_percent_ownership - LAG(avg_percent_ownership) OVER (PARTITION BY country ORDER BY year) AS difference
    FROM yearly_avg
),
running_total AS (
    SELECT year, country, 
    ROUND(SUM(difference) OVER (PARTITION BY country ORDER BY year), 2) AS cumulative_bp_change_of_ownership
    FROM difference
    WHERE difference IS NOT NULL
)

SELECT country AS "Country", cumulative_bp_change_of_ownership
FROM running_total
WHERE year = (SELECT MAX(year)FROM oil_fund)
ORDER BY cumulative_bp_change_of_ownership DESC;