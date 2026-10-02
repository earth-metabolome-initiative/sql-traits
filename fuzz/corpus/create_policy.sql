CREATE POLICY user_isolation ON users
    FOR ALL
    USING (user_id = current_user_id())
    WITH CHECK (user_id = current_user_id());

CREATE POLICY admin_all ON users
    FOR ALL
    TO admin
    USING (true);
