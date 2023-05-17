WITH yearly_mrkt_value AS (
    SELECT year, sector, SUM(market_value) as mrkt_value
FROM oil_fund
WHERE category = 'Equity' AND year > (SELECT MAX(year) - 11 FROM oil_fund)
GROUP BY year, sector
ORDER BY year, sector
), 

mrkt_value_percent_diff AS (
    SELECT year, sector, mrkt_value,
(mrkt_value - LAG(mrkt_value) OVER (PARTITION BY sector ORDER BY year))/ mrkt_value AS prct_difference
FROM yearly_mrkt_value
),

running_total_mrkt_value AS (
    SELECT year, sector,
ROUND(SUM(prct_difference) OVER (PARTITION BY sector ORDER BY year) * 100, 2) AS cumulative_change_mrkt_value
FROM mrkt_value_percent_diff
),

yearly_avg AS (
    SELECT year, sector, AVG(percent_ownership) AS avg_percent_ownership
    FROM oil_fund
    WHERE category = 'Equity' AND year > (SELECT MAX(year) - 11 FROM oil_fund)
    GROUP BY year, sector
    ORDER BY year, sector
),

yearly_avg_difference AS (
    SELECT year, sector, 
           avg_percent_ownership - LAG(avg_percent_ownership) OVER (PARTITION BY sector ORDER BY year) AS difference
    FROM yearly_avg
),

yearly_avg_running_total AS (
    SELECT year, sector, 
           ROUND(SUM(difference) OVER (PARTITION BY sector ORDER BY year) * 100, 2) AS cumulative_bp_change_of_ownership
    FROM yearly_avg_difference
    WHERE difference IS NOT NULL
)

SELECT r.sector AS "Sector",
a.cumulative_bp_change_of_ownership as "Cumulative Average Ownership Change",
r.cumulative_change_mrkt_value as "Cumulative Market Value Percent Change"
FROM (
    SELECT sector, cumulative_change_mrkt_value
    FROM running_total_mrkt_value
    WHERE year = (SELECT MAX(year) FROM oil_fund)
) r
JOIN (
    SELECT sector, cumulative_bp_change_of_ownership
    FROM yearly_avg_running_total
    WHERE year = (SELECT MAX(year) FROM oil_fund)
) a ON r.sector = a.sector
ORDER BY a.cumulative_bp_change_of_ownership DESC;
