WITH company_avg AS (
    SELECT year, country, name, percent_ownership
    FROM oil_fund
    WHERE category = 'Equity' AND year > (SELECT MAX(year) - (3 + 1) FROM oil_fund)
    AND country IN ('Canada')
),

difference AS (
    SELECT year, country, name, 
    percent_ownership - LAG(percent_ownership) OVER (PARTITION BY name ORDER BY year) AS difference
    FROM company_avg

),

running_total AS (
    SELECT year, country, name,
    ROUND(SUM(difference) OVER (PARTITION BY name ORDER BY year), 2) AS cumulative_bp_change_of_ownership
    FROM difference
    WHERE difference IS NOT NULL
),

top_10_companies AS (
    SELECT name AS "Company", cumulative_bp_change_of_ownership
FROM running_total
WHERE year = (SELECT MAX(year)FROM oil_fund)
ORDER BY cumulative_bp_change_of_ownership DESC
LIMIT 10
),

company_names_cte AS (
    SELECT "Company"
FROM top_10_companies
)

SELECT year, name, percent_ownership
FROM oil_fund
WHERE category = 'Equity'
  AND year > (SELECT MAX(year) - (3 + 1) FROM oil_fund)
  AND name IN (SELECT "Company" FROM company_names_cte);
