SELECT 
year,
CASE
    WHEN country IN ('Australia', 'Austria', 'Belgium', 'Canada', 'Denmark', 'Finland', 'France', 'Germany', 'Hong Kong', 'Ireland', 'Israel',
    'Italy', 'Japan', 'Netherlands', 'New Zealand', 'Norway', 'Portugal', 'Singapore', 'South Korea', 'Spain', 'Sweden', 'Switzerland',
    'United Kingdom', 'United States') THEN 'MSCI Developed Market'
      
      WHEN country IN ('Brazil', 'Chile', 'China', 'Colombia', 'Czech Republic', 'Egypt', 'Greece', 'Hungary', 'Indonesia', 'India', 'South Korea',
      'Mexico', 'Malaysia', 'Peru', 'Philippines', 'Poland', 'Qatar', 'Saudi Arabia', 'South Africa', 'Thailand', 'Turkey', 'Taiwan',
      'United Arab Emirates') THEN 'MSCI Emerging Market'

    ELSE 'Other'
END AS MSCI_market, 
ROUND(AVG(percent_ownership),2) AS avg_percent_ownership
FROM
oil_fund
WHERE category = 'Equity' AND
CASE
    WHEN country IN ('Australia', 'Austria', 'Belgium', 'Canada', 'Denmark', 'Finland', 'France', 'Germany', 'Hong Kong', 'Ireland', 'Israel',
     'Italy', 'Japan', 'Netherlands', 'New Zealand', 'Norway', 'Portugal', 'Singapore', 'South Korea', 'Spain', 'Sweden', 'Switzerland',
      'United Kingdom', 'United States') THEN 'MSCI Developed Market'

    WHEN country IN ('Brazil', 'Chile', 'China', 'Colombia', 'Czech Republic', 'Egypt', 'Greece', 'Hungary', 'Indonesia', 'India', 'South Korea',
     'Mexico', 'Malaysia', 'Peru', 'Philippines', 'Poland', 'Qatar', 'Saudi Arabia', 'South Africa', 'Thailand', 'Turkey', 'Taiwan',
      'United Arab Emirates') THEN 'MSCI Emerging Market'
    ELSE 'Other'
  END IN ('MSCI Developed Market', 'MSCI Emerging Market')
GROUP BY
  year,
  MSCI_market
ORDER BY
  year;


