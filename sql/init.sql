CREATE TABLE IF NOT EXISTS customers (
  customer_id        varchar PRIMARY KEY,
  customer_unique_id varchar,
  customer_city      varchar,
  customer_state     varchar,
  updated_at         timestamp DEFAULT now()
);

CREATE TABLE IF NOT EXISTS products (
  product_id         varchar PRIMARY KEY,
  product_category   varchar,
  product_weight_g   int,
  updated_at         timestamp DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
  order_id           varchar PRIMARY KEY,
  customer_id        varchar REFERENCES customers(customer_id),
  order_status       varchar,
  order_purchase_ts  timestamp,
  updated_at         timestamp DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_items (
  order_id           varchar REFERENCES orders(order_id),
  order_item_id      int,
  product_id         varchar REFERENCES products(product_id),
  price              numeric(10,2),
  shipping_limit_dt  timestamp,
  updated_at         timestamp DEFAULT now(),
  PRIMARY KEY(order_id, order_item_id)
);

INSERT INTO customers (customer_id, customer_unique_id, customer_city, customer_state) VALUES
  ('c_001','u_001','São Paulo','SP'),
  ('c_002','u_002','Rio de Janeiro','RJ')
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO products (product_id, product_category, product_weight_g) VALUES
  ('p_001','electronics', 350),
  ('p_002','books',       500)
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO orders (order_id, customer_id, order_status, order_purchase_ts) VALUES
  ('o_001','c_001','delivered', now() - interval '2 days'),
  ('o_002','c_002','shipped',   now() - interval '1 day')
ON CONFLICT (order_id) DO NOTHING;

INSERT INTO order_items (order_id, order_item_id, product_id, price, shipping_limit_dt) VALUES
  ('o_001', 1, 'p_001', 199.99, now() + interval '2 days'),
  ('o_002', 1, 'p_002',  29.99, now() + interval '3 days')
ON CONFLICT DO NOTHING;
