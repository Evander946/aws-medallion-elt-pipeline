-- Example analytics queries on the Gold layer.
SELECT date_trunc('month', order_date) AS month, sum(revenue) AS revenue
FROM daily_sales GROUP BY 1 ORDER BY 1;

SELECT country, count(*) AS customers, sum(lifetime_value) AS ltv
FROM customer_summary GROUP BY country ORDER BY ltv DESC;
