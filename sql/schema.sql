CREATE TABLE ecommerce_data (
    item_id BIGINT,
    status VARCHAR(50),
    created_at TIMESTAMP,
    sku VARCHAR(100),
    price FLOAT,
    qty_ordered INT,
    grand_total FLOAT,
    increment_id VARCHAR(50),
    category_name_1 VARCHAR(100),
    sales_commission_code VARCHAR(100),
    discount_amount FLOAT,
    payment_method VARCHAR(50),
    working_date TIMESTAMP,
    bi_status VARCHAR(50),
    m_y TIMESTAMP
);