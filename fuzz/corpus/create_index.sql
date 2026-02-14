CREATE INDEX idx_users_email ON users(email);
CREATE UNIQUE INDEX idx_orders_user_date ON orders(user_id, created_at DESC);
CREATE INDEX idx_orders_status ON orders(status) WHERE status != 'completed';
