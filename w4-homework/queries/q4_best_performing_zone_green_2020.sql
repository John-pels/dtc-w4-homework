SELECT
    pickup_zone,
    SUM(revenue_monthly_total_amount) AS total_revenue
FROM `dtc-w4-bquery.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 5;
