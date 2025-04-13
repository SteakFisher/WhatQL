-- Drop tables if they exist
DROP TABLE IF EXISTS comments;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users;

-- Create tables
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    age INTEGER
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    product TEXT NOT NULL,
    amount REAL,
    order_date TEXT,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE comments (
    id INTEGER PRIMARY KEY,
    order_id INTEGER,
    comment TEXT NOT NULL,
    rating INTEGER,
    FOREIGN KEY (order_id) REFERENCES orders(id)
);

-- Add indexes
CREATE INDEX idx_users_age ON users(age);
CREATE INDEX idx_orders_user ON orders(user_id);
CREATE INDEX idx_orders_amount ON orders(amount);

-- Insert test data
-- Users
INSERT INTO users (name, email, age) VALUES
('Alice', 'alice@example.com', 28),
('Bob', 'bob@example.com', 35),
('Charlie', 'charlie@example.com', 42),
('David', 'david@example.com', 22),
('Eve', 'eve@example.com', 31);

-- Orders
INSERT INTO orders (user_id, product, amount, order_date) VALUES
(1, 'Laptop', 1200.00, '2023-01-15'),
(1, 'Mouse', 25.50, '2023-01-16'),
(2, 'Monitor', 350.00, '2023-02-01'),
(3, 'Keyboard', 120.00, '2023-02-10'),
(3, 'Headphones', 85.00, '2023-02-15'),
(3, 'Desk Chair', 250.00, '2023-03-01'),
(4, 'USB Drive', 15.00, '2023-03-10');

-- Comments
INSERT INTO comments (order_id, comment, rating) VALUES
(1, 'Great laptop!', 5),
(1, 'Fast shipping', 5),
(2, 'Works perfectly', 4),
(3, 'Good quality', 4),
(5, 'Excellent sound', 5),
(6, 'Very comfortable', 5),
(7, 'Small but useful', 3);