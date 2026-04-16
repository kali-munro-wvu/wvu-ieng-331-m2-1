-- ABC Classification Query

SELECT
    p.product_id,
    p.product_category_name,
    COUNT(oi.order_id) AS total_items_sold,
    SUM(oi.price) AS total_revenue
FROM order_items oi
JOIN products p
    ON oi.product_id = p.product_id
JOIN orders o
    ON oi.order_id = o.order_id
WHERE ($1 IS NULL OR o.order_purchase_timestamp >= $1)
GROUP BY p.product_id, p.product_category_name