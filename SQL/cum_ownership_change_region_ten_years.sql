WITH yearly_avg AS (
    SELECT year, region, AVG(percent_ownership)::numeric AS avg_percent_ownership
    FROM oil_fund
    WHERE category = 'Equity' AND year > (SELECT MAX(year) - 11 FROM oil_fund)
    GROUP BY year, region
    ORDER BY year, region
),
difference AS (
    SELECT year, region, 
           avg_percent_ownership - LAG(avg_percent_ownership) OVER (PARTITION BY region ORDER BY year) AS difference
    FROM yearly_avg
),
running_total AS (
    SELECT year, region, 
           ROUND(SUM(difference) OVER (PARTITION BY region ORDER BY year) * 100, 2) AS cumulative_percent_change_of_ownership
    FROM difference
    WHERE difference IS NOT NULL
)

SELECT region, cumulative_percent_change_of_ownership
FROM running_total
WHERE year = (SELECT MAX(year)FROM oil_fund)
ORDER BY cumulative_percent_change_of_ownership DESC;