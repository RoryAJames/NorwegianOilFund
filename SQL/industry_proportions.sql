SELECT year, industry as "Industry",
ROUND(sum(market_value) / sum(sum(market_value)) OVER (PARTITION BY year) * 100.0,2) as "Proportion of Fund"
FROM oil_fund
GROUP BY year, industry;