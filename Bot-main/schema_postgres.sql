CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    telegram_id INTEGER UNIQUE NOT NULL,
    name TEXT NOT NULL,
    phone TEXT,
    email TEXT,
    language TEXT DEFAULT 'ru',
    is_admin INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    acquisition_channel TEXT
)

CREATE TABLE IF NOT EXISTS categories (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    emoji TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    price DOUBLE PRECISION NOT NULL,
    category_id INTEGER,
    subcategory_id INTEGER,
    brand TEXT,
    image_url TEXT,
    stock INTEGER DEFAULT 0,
    views INTEGER DEFAULT 0,
    sales_count INTEGER DEFAULT 0,
    is_active INTEGER DEFAULT 1,
    cost_price DOUBLE PRECISION DEFAULT 0,
    original_price DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES categories (id),
    FOREIGN KEY (subcategory_id) REFERENCES subcategories (id)
)

CREATE TABLE IF NOT EXISTS cart (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    product_id INTEGER,
    quantity INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id),
    FOREIGN KEY (product_id) REFERENCES products (id)
)

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    total_amount DOUBLE PRECISION,
    status TEXT DEFAULT 'pending',
    delivery_address TEXT,
    payment_method TEXT,
    payment_status TEXT DEFAULT 'pending',
    promo_discount DOUBLE PRECISION DEFAULT 0,
    delivery_cost DOUBLE PRECISION DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id)
)

CREATE TABLE IF NOT EXISTS product_images (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    image_url TEXT NOT NULL,
    sort_order INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products (id)
)

CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    price DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders (id),
    FOREIGN KEY (product_id) REFERENCES products (id)
)

CREATE TABLE IF NOT EXISTS subcategories (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    category_id INTEGER,
    emoji TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES categories (id)
)

CREATE TABLE IF NOT EXISTS reviews (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    product_id INTEGER,
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
    comment TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id),
    FOREIGN KEY (product_id) REFERENCES products (id)
)

CREATE TABLE IF NOT EXISTS favorites (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    product_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id),
    FOREIGN KEY (product_id) REFERENCES products (id),
    UNIQUE(user_id, product_id)
)

CREATE TABLE IF NOT EXISTS notifications (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    type TEXT DEFAULT 'info',
    is_read INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id)
)

CREATE TABLE IF NOT EXISTS loyalty_points (
    id SERIAL PRIMARY KEY,
    user_id INTEGER UNIQUE,
    current_points INTEGER DEFAULT 0,
    total_earned INTEGER DEFAULT 0,
    current_tier TEXT DEFAULT 'Bronze',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id)
)

CREATE TABLE IF NOT EXISTS promo_codes (
    id SERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    discount_type TEXT NOT NULL,
    discount_value DOUBLE PRECISION NOT NULL,
    min_order_amount DOUBLE PRECISION DEFAULT 0,
    max_uses INTEGER,
    expires_at TIMESTAMP,
    description TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)

CREATE TABLE IF NOT EXISTS promo_uses (
    id SERIAL PRIMARY KEY,
    promo_code_id INTEGER,
    user_id INTEGER,
    order_id INTEGER,
    discount_amount DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (promo_code_id) REFERENCES promo_codes (id),
    FOREIGN KEY (user_id) REFERENCES users (id),
    FOREIGN KEY (order_id) REFERENCES orders (id)
)

CREATE TABLE IF NOT EXISTS shipments (
    id SERIAL PRIMARY KEY,
    order_id INTEGER,
    tracking_number TEXT UNIQUE,
    delivery_provider TEXT,
    delivery_option TEXT,
    time_slot TEXT,
    status TEXT DEFAULT 'created',
    estimated_delivery TIMESTAMP,
    scheduled_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders (id)
)

CREATE TABLE IF NOT EXISTS business_expenses (
    id SERIAL PRIMARY KEY,
    expense_type TEXT NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    description TEXT,
    expense_date DATE NOT NULL,
    is_tax_deductible INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)

CREATE TABLE IF NOT EXISTS suppliers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    contact_email TEXT,
    phone TEXT,
    address TEXT,
    payment_terms TEXT,
    cost_per_unit DOUBLE PRECISION DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)

CREATE TABLE IF NOT EXISTS inventory_rules (
    id SERIAL PRIMARY KEY,
    product_id INTEGER,
    reorder_point INTEGER NOT NULL,
    reorder_quantity INTEGER NOT NULL,
    supplier_id INTEGER,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products (id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers (id)
)

CREATE TABLE IF NOT EXISTS inventory_movements (
    id SERIAL PRIMARY KEY,
    product_id INTEGER,
    movement_type TEXT NOT NULL,
    quantity_change INTEGER NOT NULL,
    old_quantity INTEGER,
    new_quantity INTEGER,
    supplier_id INTEGER,
    cost_per_unit DOUBLE PRECISION,
    reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products (id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers (id)
)

CREATE TABLE IF NOT EXISTS purchase_orders (
    id SERIAL PRIMARY KEY,
    product_id INTEGER,
    supplier_id INTEGER,
    quantity INTEGER NOT NULL,
    cost_per_unit DOUBLE PRECISION NOT NULL,
    total_amount DOUBLE PRECISION NOT NULL,
    status TEXT DEFAULT 'pending',
    received_quantity INTEGER DEFAULT 0,
    delivered_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products (id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers (id)
)

CREATE TABLE IF NOT EXISTS automation_rules (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    conditions TEXT,
    actions TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)

CREATE TABLE IF NOT EXISTS automation_executions (
    id SERIAL PRIMARY KEY,
    rule_id INTEGER,
    user_id INTEGER,
    rule_type TEXT,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (rule_id) REFERENCES automation_rules (id),
    FOREIGN KEY (user_id) REFERENCES users (id)
)

CREATE TABLE IF NOT EXISTS security_logs (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    activity_type TEXT NOT NULL,
    details TEXT,
    severity TEXT DEFAULT 'low',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id)
)

CREATE TABLE IF NOT EXISTS security_blocks (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    reason TEXT NOT NULL,
    blocked_until TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id)
)

CREATE TABLE IF NOT EXISTS api_keys (
    id SERIAL PRIMARY KEY,
    key_name TEXT NOT NULL,
    api_key TEXT UNIQUE NOT NULL,
    permissions TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)

CREATE TABLE IF NOT EXISTS webhook_logs (
    id SERIAL PRIMARY KEY,
    provider TEXT NOT NULL,
    order_id INTEGER,
    user_id INTEGER,
    status TEXT NOT NULL,
    error_message TEXT,
    payload_preview TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders (id),
    FOREIGN KEY (user_id) REFERENCES users (id)
)

CREATE TABLE IF NOT EXISTS marketing_campaigns (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    segment TEXT,
    campaign_type TEXT,
    target_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)

CREATE TABLE IF NOT EXISTS stock_reservations (
    id SERIAL PRIMARY KEY,
    product_id INTEGER,
    order_id INTEGER,
    quantity INTEGER NOT NULL,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products (id),
    FOREIGN KEY (order_id) REFERENCES orders (id)
)

CREATE TABLE IF NOT EXISTS stocktaking_sessions (
    id SERIAL PRIMARY KEY,
    location TEXT DEFAULT 'Основной склад',
    status TEXT DEFAULT 'active',
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_by INTEGER,
    FOREIGN KEY (created_by) REFERENCES users (id)
)

CREATE TABLE IF NOT EXISTS stocktaking_items (
    id SERIAL PRIMARY KEY,
    session_id INTEGER,
    product_id INTEGER,
    system_quantity INTEGER,
    counted_quantity INTEGER,
    counted_at TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES stocktaking_sessions (id),
    FOREIGN KEY (product_id) REFERENCES products (id)
)

CREATE TABLE IF NOT EXISTS user_activity_logs (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    action TEXT NOT NULL,
    search_query TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id)
)

CREATE TABLE IF NOT EXISTS flash_sale_products (
    id SERIAL PRIMARY KEY,
    promo_code_id INTEGER,
    product_id INTEGER,
    FOREIGN KEY (promo_code_id) REFERENCES promo_codes (id),
    FOREIGN KEY (product_id) REFERENCES products (id)
)

CREATE TABLE IF NOT EXISTS scheduled_posts (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    image_url TEXT,
    time_morning TEXT,
    time_afternoon TEXT,
    time_evening TEXT,
    target_audience TEXT DEFAULT 'all',
    bot_username TEXT DEFAULT 'Safar_call_bot',
    website_url TEXT DEFAULT 'https://your-website.com',
    include_reviews INTEGER DEFAULT 1,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)

CREATE TABLE IF NOT EXISTS post_statistics (
    id SERIAL PRIMARY KEY,
    post_id INTEGER,
    time_period TEXT,
    sent_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (post_id) REFERENCES scheduled_posts (id)
)