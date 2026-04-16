-- Seller Scorecard

SELECT
    s.seller_id,
    s.seller_state,
    COUNT(oi.order_id) AS total_items_sold,
    SUM(oi.price) AS total_revenue
FROM sellers s
JOIN order_items oi
    ON s.seller_id = oi.seller_id
JOIN orders o
    ON oi.order_id = o.order_id
WHERE ($1 IS NULL OR o.order_purchase_timestamp >= $1)
  AND ($2 IS NULL OR s.seller_state = $2)
GROUP BY s.seller_id, s.seller_state