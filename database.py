"""
База данных для телеграм-бота интернет-магазина
"""
import logging
from dbx import run as db_run, all as db_all, scalar as db_scalar, executemany as db_executemany
class DatabaseManager:
    def __init__(self, db_path='shop_bot.db'):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        # Обновление схемы: роль пользователя
        try:
            conn_alter = None  # sqlite removed
            cur_alter = conn_alter.cursor()
            cur_alter.execute("ALTER TABLE users ADD COLUMN role TEXT")
            conn_alter.commit()
        except Exception:
            pass
        finally:
            try:
                conn_alter.close()
            except Exception:
                pass

        """Инициализация базы данных"""
        try:
            conn = None  # sqlite removed
            # cursor removed
            
            # Создаем все таблицы
            self.create_tables(cursor)
            
            # Создаем тестовые данные если база пустая
            if self.is_database_empty(cursor):
                self.create_test_data(cursor)
            
            # commit handled
            
        except Exception as e:
            logging.info(f"Ошибка инициализации базы данных: {e}")
            if 'conn' in locals():
                conn.rollback()
        finally:
            if 'conn' in locals():
                # close handled
    
    def create_tables(self, cursor):
        """Создание всех таблиц"""
        
        # Пользователи
        self.execute_query('''
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id INTEGER UNIQUE NOT NULL,
    name TEXT NOT NULL,
    phone TEXT,
    email TEXT,
    language TEXT DEFAULT 'ru',
    is_admin INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    acquisition_channel TEXT
)
        ''')
        
        # Категории товаров
        self.execute_query('''
CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    emoji TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
        ''')
        
        # Товары
        self.execute_query('''
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    price REAL NOT NULL,
    category_id INTEGER,
    subcategory_id INTEGER,
    brand TEXT,
    image_url TEXT,
    stock INTEGER DEFAULT 0,
    views INTEGER DEFAULT 0,
    sales_count INTEGER DEFAULT 0,
    is_active INTEGER DEFAULT 1,
    cost_price REAL DEFAULT 0,
    original_price REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES categories (id),
    FOREIGN KEY (subcategory_id) REFERENCES subcategories (id)
)
        ''')
        
        # Корзина
        self.execute_query('''
CREATE TABLE IF NOT EXISTS cart (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    product_id INTEGER,
    quantity INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id),
    FOREIGN KEY (product_id) REFERENCES products (id)
)
        ''')
        
        # Заказы
        self.execute_query('''
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    total_amount REAL,
    status TEXT DEFAULT 'pending',
    delivery_address TEXT,
    payment_method TEXT,
    payment_status TEXT DEFAULT 'pending',
    promo_discount REAL DEFAULT 0,
    delivery_cost REAL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id)
)
        ''')
        
        # Товары в заказах
        # Галерея изображений товаров
        self.execute_query('''
CREATE TABLE IF NOT EXISTS product_images (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL,
    image_url TEXT NOT NULL,
    sort_order INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products (id)
)
        ''')

        # Товары в заказах
        self.execute_query('''
CREATE TABLE IF NOT EXISTS order_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    price REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders (id),
    FOREIGN KEY (product_id) REFERENCES products (id)
)
        ''')
        
        # Подкатегории/бренды
        self.execute_query('''
CREATE TABLE IF NOT EXISTS subcategories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    category_id INTEGER,
    emoji TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES categories (id)
)
        ''')
        
        # Отзывы
        self.execute_query('''
CREATE TABLE IF NOT EXISTS reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    product_id INTEGER,
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
    comment TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id),
    FOREIGN KEY (product_id) REFERENCES products (id)
)
        ''')
        
        # Избранное
        self.execute_query('''
CREATE TABLE IF NOT EXISTS favorites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    product_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id),
    FOREIGN KEY (product_id) REFERENCES products (id),
    UNIQUE(user_id, product_id)
)
        ''')
        
        # Уведомления
        self.execute_query('''
CREATE TABLE IF NOT EXISTS notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    type TEXT DEFAULT 'info',
    is_read INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id)
)
        ''')
        
        # Баллы лояльности
        self.execute_query('''
CREATE TABLE IF NOT EXISTS loyalty_points (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER UNIQUE,
    current_points INTEGER DEFAULT 0,
    total_earned INTEGER DEFAULT 0,
    current_tier TEXT DEFAULT 'Bronze',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id)
)
        ''')
        
        # Промокоды
        self.execute_query('''
CREATE TABLE IF NOT EXISTS promo_codes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT UNIQUE NOT NULL,
    discount_type TEXT NOT NULL,
    discount_value REAL NOT NULL,
    min_order_amount REAL DEFAULT 0,
    max_uses INTEGER,
    expires_at TIMESTAMP,
    description TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
        ''')
        
        # Использование промокодов
        self.execute_query('''
CREATE TABLE IF NOT EXISTS promo_uses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    promo_code_id INTEGER,
    user_id INTEGER,
    order_id INTEGER,
    discount_amount REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (promo_code_id) REFERENCES promo_codes (id),
    FOREIGN KEY (user_id) REFERENCES users (id),
    FOREIGN KEY (order_id) REFERENCES orders (id)
)
        ''')
        
        # Отгрузки
        self.execute_query('''
CREATE TABLE IF NOT EXISTS shipments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        ''')
        
        # Бизнес расходы
        self.execute_query('''
CREATE TABLE IF NOT EXISTS business_expenses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    expense_type TEXT NOT NULL,
    amount REAL NOT NULL,
    description TEXT,
    expense_date DATE NOT NULL,
    is_tax_deductible INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
        ''')
        
        # Поставщики
        self.execute_query('''
CREATE TABLE IF NOT EXISTS suppliers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    contact_email TEXT,
    phone TEXT,
    address TEXT,
    payment_terms TEXT,
    cost_per_unit REAL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
        ''')
        
        # Правила инвентаризации
        self.execute_query('''
CREATE TABLE IF NOT EXISTS inventory_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER,
    reorder_point INTEGER NOT NULL,
    reorder_quantity INTEGER NOT NULL,
    supplier_id INTEGER,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products (id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers (id)
)
        ''')
        
        # Движения товаров
        self.execute_query('''
CREATE TABLE IF NOT EXISTS inventory_movements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER,
    movement_type TEXT NOT NULL,
    quantity_change INTEGER NOT NULL,
    old_quantity INTEGER,
    new_quantity INTEGER,
    supplier_id INTEGER,
    cost_per_unit REAL,
    reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products (id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers (id)
)
        ''')
        
        # Заказы поставщикам
        self.execute_query('''
CREATE TABLE IF NOT EXISTS purchase_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER,
    supplier_id INTEGER,
    quantity INTEGER NOT NULL,
    cost_per_unit REAL NOT NULL,
    total_amount REAL NOT NULL,
    status TEXT DEFAULT 'pending',
    received_quantity INTEGER DEFAULT 0,
    delivered_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products (id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers (id)
)
        ''')
        
        # Правила автоматизации
        self.execute_query('''
CREATE TABLE IF NOT EXISTS automation_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    conditions TEXT,
    actions TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
        ''')
        
        # Выполнения автоматизации
        self.execute_query('''
CREATE TABLE IF NOT EXISTS automation_executions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id INTEGER,
    user_id INTEGER,
    rule_type TEXT,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (rule_id) REFERENCES automation_rules (id),
    FOREIGN KEY (user_id) REFERENCES users (id)
)
        ''')
        
        # Логи безопасности
        self.execute_query('''
CREATE TABLE IF NOT EXISTS security_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    activity_type TEXT NOT NULL,
    details TEXT,
    severity TEXT DEFAULT 'low',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id)
)
        ''')
        
        # Блокировки пользователей
        self.execute_query('''
CREATE TABLE IF NOT EXISTS security_blocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    reason TEXT NOT NULL,
    blocked_until TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id)
)
        ''')
        
        # API ключи
        self.execute_query('''
CREATE TABLE IF NOT EXISTS api_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key_name TEXT NOT NULL,
    api_key TEXT UNIQUE NOT NULL,
    permissions TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
        ''')
        
        # Логи webhook'ов
        self.execute_query('''
CREATE TABLE IF NOT EXISTS webhook_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        ''')
        
        # Маркетинговые кампании
        self.execute_query('''
CREATE TABLE IF NOT EXISTS marketing_campaigns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    segment TEXT,
    campaign_type TEXT,
    target_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
        ''')
        
        # Резервы товаров
        self.execute_query('''
CREATE TABLE IF NOT EXISTS stock_reservations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER,
    order_id INTEGER,
    quantity INTEGER NOT NULL,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products (id),
    FOREIGN KEY (order_id) REFERENCES orders (id)
)
        ''')
        
        # Сессии инвентаризации
        self.execute_query('''
CREATE TABLE IF NOT EXISTS stocktaking_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    location TEXT DEFAULT 'Основной склад',
    status TEXT DEFAULT 'active',
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_by INTEGER,
    FOREIGN KEY (created_by) REFERENCES users (id)
)
        ''')
        
        # Элементы инвентаризации
        self.execute_query('''
CREATE TABLE IF NOT EXISTS stocktaking_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER,
    product_id INTEGER,
    system_quantity INTEGER,
    counted_quantity INTEGER,
    counted_at TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES stocktaking_sessions (id),
    FOREIGN KEY (product_id) REFERENCES products (id)
)
        ''')
        
        # Логи активности пользователей
        self.execute_query('''
CREATE TABLE IF NOT EXISTS user_activity_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    action TEXT NOT NULL,
    search_query TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id)
)
        ''')
        
        # Товары флеш-распродажи
        self.execute_query('''
CREATE TABLE IF NOT EXISTS flash_sale_products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    promo_code_id INTEGER,
    product_id INTEGER,
    FOREIGN KEY (promo_code_id) REFERENCES promo_codes (id),
    FOREIGN KEY (product_id) REFERENCES products (id)
)
        ''')
        
        # Запланированные посты
        self.execute_query('''
CREATE TABLE IF NOT EXISTS scheduled_posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        ''')
        
        # Статистика постов
        self.execute_query('''
CREATE TABLE IF NOT EXISTS post_statistics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id INTEGER,
    time_period TEXT,
    sent_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (post_id) REFERENCES scheduled_posts (id)
)
        ''')
        
        # Создаем индексы для оптимизации
        self.create_indexes(cursor)
    
    def create_indexes(self, cursor):
        """Создание индексов для оптимизации"""
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)',
            'CREATE INDEX IF NOT EXISTS idx_products_category ON products(category_id)',
            'CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id)',
            'CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)',
            'CREATE INDEX IF NOT EXISTS idx_cart_user ON cart(user_id)',
            'CREATE INDEX IF NOT EXISTS idx_reviews_product ON reviews(product_id)',
            'CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id)',
            'CREATE INDEX IF NOT EXISTS idx_inventory_movements_product ON inventory_movements(product_id)',
            'CREATE INDEX IF NOT EXISTS idx_security_logs_user ON security_logs(user_id)',
            'CREATE INDEX IF NOT EXISTS idx_automation_executions_user ON automation_executions(user_id)'
        ]
        
        for index_sql in indexes:
            try:
                self.execute_query(index_sql)
            except Exception as e:
                logging.info(f"Ошибка создания индекса: {e}")
    
    def is_database_empty(self, cursor):
        """Проверка пустоты базы данных"""
        self.execute_query('SELECT COUNT(*) FROM categories')
        return cursor.fetchone()[0] == 0
    
    def create_test_data(self, cursor):
        """Создание тестовых данных"""
        # Создаем админа из переменных окружения
        from config import BOT_CONFIG
        admin_telegram_id = BOT_CONFIG.get('admin_telegram_id')
        admin_name = BOT_CONFIG.get('admin_name', 'Admin')
        
        if admin_telegram_id:
            try:
                admin_telegram_id = int(admin_telegram_id)
                self.execute_query('''
                    INSERT OR IGNORE INTO users (telegram_id, name, is_admin, language, created_at)
                    VALUES (?, ?, 1, 'ru', CURRENT_TIMESTAMP)
                ''', (admin_telegram_id, admin_name))
                logging.info(f"✅ Админ создан: {admin_name} (ID: {admin_telegram_id})")
            except ValueError:
                logging.info(f"⚠️ Неверный ADMIN_TELEGRAM_ID: {admin_telegram_id}")
        else:
            logging.info("⚠️ ADMIN_TELEGRAM_ID не установлен в конфигурации")
        
        # Категории
        categories = [
            ('Электроника', 'Смартфоны, ноутбуки, гаджеты', '📱'),
            ('Одежда', 'Мужская и женская одежда', '👕'),
            ('Дом и сад', 'Товары для дома и дачи', '🏠'),
            ('Спорт', 'Спортивные товары и инвентарь', '⚽'),
            ('Красота', 'Косметика и парфюмерия', '💄'),
            ('Книги', 'Художественная и техническая литература', '📚')
        ]
        
        db_executemany(
            'INSERT INTO categories (name, description, emoji) VALUES (?, ?, ?)',
            categories
        )
        
        # Подкатегории/бренды
        subcategories = [
            ('Apple', 1, '🍎'),      # Электроника - Apple
            ('Samsung', 1, '📱'),    # Электроника - Samsung
            ('Nike', 2, '✔️'),       # Одежда - Nike
            ('Levi\'s', 2, '👖'),    # Одежда - Levi's
            ('Delonghi', 3, '☕'),   # Дом и сад - Delonghi
            ('Adidas', 4, '👟'),     # Спорт - Adidas
            ('Chanel', 5, '💎'),     # Красота - Chanel
            ('Книги', 6, '📖')       # Книги - общие
        ]
        
        db_executemany(
            'INSERT INTO subcategories (name, category_id, emoji) VALUES (?, ?, ?)',
            subcategories
        )
        
        # Товары
        products = [
            ('iPhone 14', 'Смартфон Apple iPhone 14 128GB', 799.99, 1, 1, 'Apple', 'https://images.pexels.com/photos/788946/pexels-photo-788946.jpeg', 50, 0, 0, 1, 600.00),
            ('Samsung Galaxy S23', 'Флагманский смартфон Samsung', 699.99, 1, 1, 'Samsung', 'https://images.pexels.com/photos/1092644/pexels-photo-1092644.jpeg', 30, 0, 0, 1, 500.00),
            ('MacBook Air M2', 'Ноутбук Apple MacBook Air с чипом M2', 1199.99, 1, 1, 'Apple', 'https://images.pexels.com/photos/18105/pexels-photo.jpg', 20, 0, 0, 1, 900.00),
            ('Футболка Nike', 'Спортивная футболка Nike Dri-FIT', 29.99, 2, 2, 'Nike', 'https://images.pexels.com/photos/8532616/pexels-photo-8532616.jpeg', 100, 0, 0, 1, 15.00),
            ('Джинсы Levi\'s', 'Классические джинсы Levi\'s 501', 79.99, 2, 2, 'Levi\'s', 'https://images.pexels.com/photos/1598507/pexels-photo-1598507.jpeg', 75, 0, 0, 1, 40.00),
            ('Кофеварка Delonghi', 'Автоматическая кофеварка', 299.99, 3, 3, 'Delonghi', 'https://images.pexels.com/photos/324028/pexels-photo-324028.jpeg', 25, 0, 0, 1, 180.00),
            ('Набор посуды', 'Набор кастрюль из нержавеющей стали', 149.99, 3, 3, 'Generic', 'https://images.pexels.com/photos/4226796/pexels-photo-4226796.jpeg', 40, 0, 0, 1, 80.00),
            ('Кроссовки Adidas', 'Беговые кроссовки Adidas Ultraboost', 159.99, 4, 4, 'Adidas', 'https://images.pexels.com/photos/2529148/pexels-photo-2529148.jpeg', 60, 0, 0, 1, 90.00),
            ('Гантели 10кг', 'Набор гантелей для домашних тренировок', 89.99, 4, 4, 'Generic', 'https://images.pexels.com/photos/416717/pexels-photo-416717.jpeg', 35, 0, 0, 1, 50.00),
            ('Крем для лица', 'Увлажняющий крем с гиалуроновой кислотой', 49.99, 5, 5, 'Generic', 'https://images.pexels.com/photos/3685530/pexels-photo-3685530.jpeg', 80, 0, 0, 1, 25.00),
            ('Парфюм Chanel', 'Туалетная вода Chanel No.5', 129.99, 5, 5, 'Chanel', 'https://images.pexels.com/photos/965989/pexels-photo-965989.jpeg', 45, 0, 0, 1, 70.00),
            ('Книга "Python для начинающих"', 'Учебник по программированию на Python', 39.99, 6, 6, 'Generic', 'https://images.pexels.com/photos/159711/books-bookstore-book-reading-159711.jpeg', 90, 0, 0, 1, 20.00),
            ('Роман "1984"', 'Классический роман Джорджа Оруэлла', 19.99, 6, 6, 'Generic', 'https://images.pexels.com/photos/46274/pexels-photo-46274.jpeg', 120, 0, 0, 1, 10.00),
            ('Беспроводные наушники', 'AirPods Pro с шумоподавлением', 249.99, 1, 1, 'Apple', 'https://images.pexels.com/photos/3394650/pexels-photo-3394650.jpeg', 70, 0, 0, 1, 150.00)
        ]
        
        db_executemany('''
            INSERT INTO products (name, description, price, category_id, subcategory_id, brand, image_url, stock, views, sales_count, is_active, cost_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', products)
        
        # Промокоды
        promo_codes = [
            ('WELCOME10', 'percentage', 10, 0, None, None, 'Скидка 10% для новых клиентов'),
            ('SAVE20', 'percentage', 20, 100, 100, None, 'Скидка 20% при заказе от $100'),
            ('FIXED15', 'fixed', 15, 50, 50, None, 'Скидка $15 при заказе от $50'),
            ('VIP25', 'percentage', 25, 200, 20, None, 'VIP скидка 25% для заказов от $200')
        ]
        
        db_executemany('''
            INSERT INTO promo_codes (code, discount_type, discount_value, min_order_amount, max_uses, expires_at, description)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', promo_codes)
        
        # Поставщики
        suppliers = [
            ('Apple Inc.', 'supplier@apple.com', '+1-800-275-2273', 'Cupertino, CA', 'NET 30'),
            ('Samsung Electronics', 'b2b@samsung.com', '+82-2-2255-0114', 'Seoul, South Korea', 'NET 45'),
            ('Nike Inc.', 'wholesale@nike.com', '+1-503-671-6453', 'Beaverton, OR', 'NET 30'),
            ('Местный поставщик', 'local@supplier.uz', '+998-71-123-4567', 'Ташкент, Узбекистан', 'Предоплата')
        ]
        
        db_executemany('''
            INSERT INTO suppliers (name, contact_email, phone, address, payment_terms)
            VALUES (?, ?, ?, ?, ?)
        ''', suppliers)
    
    
    
def _sql_fixups(sql: str) -> str:
    # SQLite → Postgres conversions
    import re as _re
    s = sql
    s = s.replace("IFNULL(", "COALESCE(").replace("ifnull(", "COALESCE(")
    s = s.replace("datetime('now')", "CURRENT_TIMESTAMP")
    # strftime('%Y-%m', ts) → to_char(ts, 'YYYY-MM')
    s = _re.sub(r"strftime\('%Y-%m'\s*,\s*([^)]+)\)", r"to_char(\1, 'YYYY-MM')", s)
    # AUTOINCREMENT primary keys
    s = _re.sub(r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT", "SERIAL PRIMARY KEY", s, flags=_re.IGNORECASE)
    s = _re.sub(r"INTEGER\s+PRIMARY\s+KEY", "SERIAL PRIMARY KEY", s, flags=_re.IGNORECASE)
    # REAL → DOUBLE PRECISION
    s = _re.sub(r"\bREAL\b", "DOUBLE PRECISION", s, flags=_re.IGNORECASE)
    return s

def _qmark_to_named(sql: str, params):
    if params is None:
        return sql, {}
    if isinstance(params, dict):
        return sql, params
    named = {}
    out = []
    i = 0
    for ch in sql:
        if ch == '?':
            key = f"p{i}"
            out.append(f":{key}")
            i += 1
        else:
            out.append(ch)
    for j, v in enumerate(params):
        named[f"p{j}"] = v
    return ''.join(out), named

from sqlalchemy import text as _text
from dbx import engine as _engine

def execute_query(self, query, params=None):
    """Postgres execution via SQLAlchemy; сохраняет старый контракт:
    SELECT -> list[tuple]; INSERT -> lastrowid (int); UPDATE/DELETE -> rowcount (int or 0).
    """
    try:
        q = _sql_fixups(query.strip())
        q_type = q.split()[0].upper() if q else ""
        # auto append RETURNING id for INSERT без returning
        need_returning_id = (q_type == "INSERT") and (" RETURNING " not in q.upper())
        q_mod = q + " RETURNING id" if need_returning_id else q
        q_sql, bind = _qmark_to_named(q_mod, params)

        if q_type == "SELECT":
            rows = db_all(q_sql, bind)
            return [tuple(r.values()) for r in rows]
        else:
            with _engine.begin() as con:
                res = con.execute(_text(q_sql), bind)
                if q_type == "INSERT":
                    try:
                        rid = res.scalar()
                        return int(rid) if rid is not None else None
                    except Exception:
                        return None
                else:
                    try:
                        return res.rowcount or 0
                    except Exception:
                        return 0
    except Exception as e:
        logging.info(f"Ошибка выполнения запроса: {e}")
        return None
