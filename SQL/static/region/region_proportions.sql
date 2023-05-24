SELECT year, region as "Region",
ROUND(sum(market_value) / sum(sum(market_value)) OVER (PARTITION BY year) * 100,2) as proportion
FROM oil_fund
GROUP BY year, region