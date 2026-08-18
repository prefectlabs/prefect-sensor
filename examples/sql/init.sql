CREATE TABLE orders (
    id BIGSERIAL PRIMARY KEY,
    order_id TEXT NOT NULL,
    status TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL
);
