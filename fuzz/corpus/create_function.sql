CREATE FUNCTION calculate_total(order_id INTEGER)
RETURNS DECIMAL(10, 2)
LANGUAGE SQL
AS $$
    SELECT SUM(amount) FROM order_items WHERE order_id = $1;
$$;
