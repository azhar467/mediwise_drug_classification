-- ============================================================
--  SQL Practice Database — MySQL 8 dialect
--  A small payments / e-commerce business
--  Tables: employees, customers, products, orders, order_items, payments
--
--  Load (CLI):      mysql -u <user> -p < schema_and_seed_mysql.sql
--  Load (DBeaver):  open this file -> Execute SQL Script (Alt+X)
-- ============================================================

DROP DATABASE IF EXISTS sql_practice;
CREATE DATABASE sql_practice CHARACTER SET utf8mb4;
USE sql_practice;

CREATE TABLE employees (
  employee_id INT PRIMARY KEY,
  name        VARCHAR(100) NOT NULL,
  title       VARCHAR(100),
  department  VARCHAR(50),
  manager_id  INT NULL,
  hire_date   DATE,
  salary      INT,
  CONSTRAINT fk_emp_mgr FOREIGN KEY (manager_id) REFERENCES employees(employee_id)
);

CREATE TABLE customers (
  customer_id INT PRIMARY KEY,
  name        VARCHAR(100) NOT NULL,
  email       VARCHAR(150),
  city        VARCHAR(50),
  country     VARCHAR(50),
  segment     VARCHAR(20),          -- Consumer / SME / Enterprise
  signup_date DATE
);

CREATE TABLE products (
  product_id INT PRIMARY KEY,
  name       VARCHAR(100) NOT NULL,
  category   VARCHAR(50),
  price      DECIMAL(10,2),
  cost       DECIMAL(10,2) NULL    -- one product has NULL cost on purpose
);

CREATE TABLE orders (
  order_id    INT PRIMARY KEY,
  customer_id INT,
  order_date  DATE,
  status      VARCHAR(20),          -- PLACED/SHIPPED/DELIVERED/CANCELLED/RETURNED
  channel     VARCHAR(10),          -- WEB/MOBILE/STORE
  CONSTRAINT fk_ord_cust FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE order_items (
  order_item_id INT PRIMARY KEY,
  order_id      INT,
  product_id    INT,
  quantity      INT,
  unit_price    DECIMAL(10,2),
  CONSTRAINT fk_oi_ord  FOREIGN KEY (order_id)   REFERENCES orders(order_id),
  CONSTRAINT fk_oi_prod FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE payments (
  payment_id   INT PRIMARY KEY,
  order_id     INT,
  amount       DECIMAL(12,2),
  payment_date DATE,
  method       VARCHAR(20),         -- CARD/UPI/NETBANKING/WALLET/COD
  status       VARCHAR(20),         -- SUCCESS/FAILED/REFUNDED
  CONSTRAINT fk_pay_ord FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- ---------- employees ----------
INSERT INTO employees VALUES
(1, 'Asha Rao', 'CEO', 'Executive', NULL, '2018-01-15', 320000),
(2, 'Vikram Sethi', 'VP Engineering', 'Engineering', 1, '2018-03-01', 240000),
(3, 'Meera Nair', 'VP Sales', 'Sales', 1, '2018-05-20', 230000),
(4, 'Rohan Das', 'Engineering Manager', 'Engineering', 2, '2019-02-11', 180000),
(5, 'Sara Khan', 'Engineering Manager', 'Engineering', 2, '2019-07-30', 178000),
(6, 'Arjun Mehta', 'Senior Engineer', 'Engineering', 4, '2020-01-10', 140000),
(7, 'Priya Iyer', 'Engineer', 'Engineering', 4, '2021-06-15', 110000),
(8, 'Karan Singh', 'Engineer', 'Engineering', 5, '2021-09-01', 108000),
(9, 'Neha Gupta', 'Sales Manager', 'Sales', 3, '2020-03-12', 130000),
(10, 'Imran Ali', 'Sales Rep', 'Sales', 9, '2022-02-20', 85000),
(11, 'Divya Menon', 'Sales Rep', 'Sales', 9, '2022-08-05', 82000),
(12, 'Tom Fernandes', 'Analyst', 'Sales', 3, '2023-01-09', 90000);

-- ---------- customers ----------
INSERT INTO customers VALUES
(1, 'Aarav B.', 'aarav1@example.com', 'Bengaluru', 'India', 'Enterprise', '2022-04-06'),
(2, 'Bhavna C.', 'bhavna2@example.com', 'Bengaluru', 'India', 'SME', '2022-07-11'),
(3, 'Chetan D.', 'chetan3@example.com', 'Bengaluru', 'India', 'Consumer', '2022-10-16'),
(4, 'Deepa E.', 'deepa4@example.com', 'Bengaluru', 'India', 'Enterprise', '2022-01-21'),
(5, 'Esha F.', 'esha5@example.com', 'Mumbai', 'India', 'SME', '2022-04-26'),
(6, 'Farhan G.', 'farhan6@example.com', 'Mumbai', 'India', 'Consumer', '2022-07-04'),
(7, 'Gita H.', 'gita7@example.com', 'Mumbai', 'India', 'Enterprise', '2022-10-09'),
(8, 'Hari I.', 'hari8@example.com', 'Delhi', 'India', 'SME', '2022-01-14'),
(9, 'Ishaan J.', 'ishaan9@example.com', 'Delhi', 'India', 'Consumer', '2023-04-19'),
(10, 'Jaya K.', 'jaya10@example.com', 'Delhi', 'India', 'Enterprise', '2023-07-24'),
(11, 'Kabir L.', 'kabir11@example.com', 'Chennai', 'India', 'SME', '2023-10-02'),
(12, 'Lata M.', 'lata12@example.com', 'Chennai', 'India', 'Consumer', '2023-01-07'),
(13, 'Manish N.', 'manish13@example.com', 'Pune', 'India', 'Enterprise', '2023-04-12'),
(14, 'Nisha O.', 'nisha14@example.com', 'Pune', 'India', 'SME', '2023-07-17'),
(15, 'Omar P.', 'omar15@example.com', 'London', 'UK', 'Consumer', '2023-10-22'),
(16, 'Pooja Q.', 'pooja16@example.com', 'London', 'UK', 'Enterprise', '2023-01-27'),
(17, 'Quasim R.', 'quasim17@example.com', 'Manchester', 'UK', 'SME', '2024-04-05'),
(18, 'Riya S.', 'riya18@example.com', 'New York', 'USA', 'Consumer', '2024-07-10'),
(19, 'Sahil T.', 'sahil19@example.com', 'New York', 'USA', 'Enterprise', '2024-10-15'),
(20, 'Tara U.', 'tara20@example.com', 'Berlin', 'Germany', 'SME', '2024-01-20');

-- ---------- products ----------
INSERT INTO products VALUES
(1, 'Wireless Mouse', 'Electronics', 899, 400),
(2, '4K Monitor', 'Electronics', 32000, 21000),
(3, 'Noise-Cancel Headphones', 'Electronics', 24000, 15000),
(4, 'Webcam', 'Electronics', 4500, 2600),
(5, 'Mechanical Keyboard', 'Accessories', 6500, 3800),
(6, 'USB-C Hub', 'Accessories', 3200, 1700),
(7, 'Laptop Stand', 'Accessories', 2200, 1100),
(8, 'Desk Lamp', 'Home', 1500, 700),
(9, 'Office Chair', 'Home', 12000, 7000),
(10, 'Standing Desk', 'Home', 28000, 18000),
(11, 'SQL Cookbook', 'Books', 1800, 900),
(12, 'Java Concurrency in Practice', 'Books', 2400, 1300),
(13, 'Cloud IDE License', 'Software', 9000, NULL),
(14, 'Backup Service', 'Software', 1200, 500),
(15, 'Cable Organizer', 'Accessories', 350, 150);

-- ---------- orders ----------
INSERT INTO orders VALUES
(1, 8, '2023-01-18', 'SHIPPED', 'MOBILE'),
(2, 15, '2023-01-31', 'DELIVERED', 'STORE'),
(3, 13, '2023-02-13', 'DELIVERED', 'WEB'),
(4, 11, '2023-02-26', 'SHIPPED', 'MOBILE'),
(5, 18, '2023-03-11', 'CANCELLED', 'STORE'),
(6, 7, '2023-03-24', 'SHIPPED', 'WEB'),
(7, 14, '2023-04-06', 'DELIVERED', 'MOBILE'),
(8, 3, '2023-04-19', 'DELIVERED', 'STORE'),
(9, 10, '2023-05-02', 'SHIPPED', 'WEB'),
(10, 17, '2023-05-15', 'PLACED', 'MOBILE'),
(11, 6, '2023-05-28', 'SHIPPED', 'STORE'),
(12, 13, '2023-06-10', 'RETURNED', 'WEB'),
(13, 2, '2023-06-23', 'DELIVERED', 'MOBILE'),
(14, 9, '2023-07-06', 'SHIPPED', 'STORE'),
(15, 16, '2023-07-19', 'PLACED', 'WEB'),
(16, 5, '2023-08-01', 'SHIPPED', 'MOBILE'),
(17, 12, '2023-08-14', 'DELIVERED', 'STORE'),
(18, 1, '2023-08-27', 'DELIVERED', 'WEB'),
(19, 8, '2023-09-09', 'SHIPPED', 'MOBILE'),
(20, 15, '2023-09-22', 'PLACED', 'STORE'),
(21, 4, '2023-10-05', 'SHIPPED', 'WEB'),
(22, 11, '2023-10-18', 'DELIVERED', 'MOBILE'),
(23, 18, '2023-10-31', 'CANCELLED', 'STORE'),
(24, 7, '2023-11-13', 'SHIPPED', 'WEB'),
(25, 14, '2023-11-26', 'PLACED', 'MOBILE');
INSERT INTO orders VALUES
(26, 3, '2023-12-09', 'SHIPPED', 'STORE'),
(27, 10, '2023-12-22', 'DELIVERED', 'WEB'),
(28, 17, '2024-01-04', 'DELIVERED', 'MOBILE'),
(29, 6, '2024-01-17', 'SHIPPED', 'STORE'),
(30, 1, '2024-01-30', 'PLACED', 'WEB'),
(31, 2, '2024-02-12', 'SHIPPED', 'MOBILE'),
(32, 9, '2024-02-25', 'DELIVERED', 'STORE'),
(33, 16, '2024-03-09', 'DELIVERED', 'WEB'),
(34, 5, '2024-03-22', 'RETURNED', 'MOBILE'),
(35, 12, '2024-04-04', 'PLACED', 'STORE'),
(36, 1, '2024-04-17', 'SHIPPED', 'WEB'),
(37, 8, '2024-04-30', 'DELIVERED', 'MOBILE'),
(38, 15, '2024-05-13', 'DELIVERED', 'STORE'),
(39, 4, '2024-05-26', 'SHIPPED', 'WEB'),
(40, 11, '2024-06-08', 'PLACED', 'MOBILE'),
(41, 18, '2024-06-21', 'SHIPPED', 'STORE'),
(42, 7, '2024-07-04', 'DELIVERED', 'WEB'),
(43, 14, '2024-07-17', 'DELIVERED', 'MOBILE'),
(44, 3, '2024-07-30', 'SHIPPED', 'STORE'),
(45, 10, '2024-08-12', 'PLACED', 'WEB'),
(46, 17, '2024-08-25', 'SHIPPED', 'MOBILE'),
(47, 6, '2024-09-07', 'DELIVERED', 'STORE'),
(48, 1, '2024-09-20', 'DELIVERED', 'WEB'),
(49, 2, '2024-10-03', 'SHIPPED', 'MOBILE'),
(50, 9, '2024-10-16', 'PLACED', 'STORE');

-- ---------- order_items ----------
INSERT INTO order_items VALUES
(1, 1, 4, 2, 4500),
(2, 1, 5, 3, 6500),
(3, 2, 7, 3, 2200),
(4, 2, 8, 1, 1500),
(5, 2, 9, 2, 10800),
(6, 3, 10, 1, 28000),
(7, 4, 13, 2, 8100),
(8, 4, 14, 3, 1200),
(9, 5, 2, 3, 32000),
(10, 5, 3, 1, 24000),
(11, 5, 4, 2, 4500),
(12, 6, 5, 1, 6500),
(13, 7, 8, 2, 1500),
(14, 7, 9, 3, 10800),
(15, 8, 11, 3, 1620),
(16, 8, 12, 1, 2400),
(17, 8, 13, 2, 9000),
(18, 9, 14, 1, 1200),
(19, 10, 3, 2, 24000),
(20, 10, 4, 3, 4500),
(21, 11, 6, 3, 3200),
(22, 11, 7, 1, 1980),
(23, 11, 8, 2, 1500),
(24, 12, 9, 1, 10800),
(25, 13, 12, 2, 2400);
INSERT INTO order_items VALUES
(26, 13, 13, 3, 9000),
(27, 14, 1, 3, 899),
(28, 14, 2, 1, 32000),
(29, 14, 3, 2, 21600),
(30, 15, 4, 1, 4500),
(31, 16, 7, 2, 1980),
(32, 16, 8, 3, 1500),
(33, 17, 10, 3, 28000),
(34, 17, 11, 1, 1800),
(35, 17, 12, 2, 2400),
(36, 18, 13, 1, 9000),
(37, 19, 2, 2, 32000),
(38, 19, 3, 3, 21600),
(39, 20, 5, 3, 5850),
(40, 20, 6, 1, 3200),
(41, 20, 7, 2, 2200),
(42, 21, 8, 1, 1500),
(43, 22, 11, 2, 1800),
(44, 22, 12, 3, 2400),
(45, 23, 14, 3, 1200),
(46, 23, 1, 1, 809),
(47, 23, 2, 2, 32000),
(48, 24, 3, 1, 21600),
(49, 25, 6, 2, 3200),
(50, 25, 7, 3, 2200);
INSERT INTO order_items VALUES
(51, 26, 9, 3, 12000),
(52, 26, 10, 1, 28000),
(53, 26, 11, 2, 1620),
(54, 27, 12, 1, 2400),
(55, 28, 1, 2, 809),
(56, 28, 2, 3, 32000),
(57, 29, 4, 3, 4500),
(58, 29, 5, 1, 6500),
(59, 29, 6, 2, 3200),
(60, 30, 7, 1, 2200),
(61, 31, 10, 2, 28000),
(62, 31, 11, 3, 1620),
(63, 32, 13, 3, 8100),
(64, 32, 14, 1, 1200),
(65, 32, 1, 2, 899),
(66, 33, 2, 1, 32000),
(67, 34, 5, 2, 6500),
(68, 34, 6, 3, 3200),
(69, 35, 8, 3, 1500),
(70, 35, 9, 1, 10800),
(71, 35, 10, 2, 28000),
(72, 36, 11, 1, 1620),
(73, 37, 14, 2, 1200),
(74, 37, 1, 3, 899),
(75, 38, 3, 3, 24000);
INSERT INTO order_items VALUES
(76, 38, 4, 1, 4500),
(77, 38, 5, 2, 5850),
(78, 39, 6, 1, 3200),
(79, 40, 9, 2, 10800),
(80, 40, 10, 3, 28000),
(81, 41, 12, 3, 2400),
(82, 41, 13, 1, 9000),
(83, 41, 14, 2, 1200),
(84, 42, 1, 1, 899),
(85, 43, 4, 2, 4500),
(86, 43, 5, 3, 5850),
(87, 44, 7, 3, 1980),
(88, 44, 8, 1, 1500),
(89, 44, 9, 2, 12000),
(90, 45, 10, 1, 28000),
(91, 46, 13, 2, 9000),
(92, 46, 14, 3, 1200),
(93, 47, 2, 3, 32000),
(94, 47, 3, 1, 21600),
(95, 47, 4, 2, 4500),
(96, 48, 5, 1, 5850),
(97, 49, 8, 2, 1500),
(98, 49, 9, 3, 12000),
(99, 50, 11, 3, 1800),
(100, 50, 12, 1, 2400);
INSERT INTO order_items VALUES
(101, 50, 13, 2, 8100);

-- ---------- payments ----------
INSERT INTO payments VALUES
(1, 1, 28500, '2023-01-19', 'UPI', 'SUCCESS'),
(2, 2, 29700, '2023-02-01', 'NETBANKING', 'SUCCESS'),
(3, 3, 28000, '2023-02-14', 'WALLET', 'SUCCESS'),
(4, 4, 19800, '2023-02-27', 'UPI', 'SUCCESS'),
(5, 6, 6500, '2023-03-25', 'CARD', 'SUCCESS'),
(6, 7, 35400, '2023-04-07', 'UPI', 'SUCCESS'),
(7, 8, 25260, '2023-04-19', 'NETBANKING', 'FAILED'),
(8, 8, 25260, '2023-04-20', 'NETBANKING', 'SUCCESS'),
(9, 9, 1200, '2023-05-03', 'WALLET', 'SUCCESS'),
(10, 10, 61500, '2023-05-16', 'UPI', 'SUCCESS'),
(11, 11, 14580, '2023-05-29', 'COD', 'SUCCESS'),
(12, 12, 10800, '2023-06-10', 'CARD', 'SUCCESS'),
(13, 12, 10800, '2023-06-14', 'CARD', 'REFUNDED'),
(14, 13, 31800, '2023-06-24', 'UPI', 'SUCCESS'),
(15, 14, 77897, '2023-07-07', 'NETBANKING', 'SUCCESS'),
(16, 15, 4500, '2023-07-20', 'WALLET', 'SUCCESS'),
(17, 16, 8460, '2023-08-02', 'UPI', 'SUCCESS'),
(18, 17, 90600, '2023-08-14', 'COD', 'FAILED'),
(19, 17, 90600, '2023-08-15', 'COD', 'SUCCESS'),
(20, 18, 9000, '2023-08-28', 'CARD', 'SUCCESS'),
(21, 19, 128800, '2023-09-10', 'UPI', 'SUCCESS'),
(22, 20, 25150, '2023-09-23', 'NETBANKING', 'SUCCESS'),
(23, 21, 1500, '2023-10-06', 'WALLET', 'SUCCESS'),
(24, 22, 10800, '2023-10-19', 'UPI', 'SUCCESS'),
(25, 24, 21600, '2023-11-14', 'CARD', 'SUCCESS');
INSERT INTO payments VALUES
(26, 25, 13000, '2023-11-27', 'UPI', 'SUCCESS'),
(27, 26, 67240, '2023-12-10', 'NETBANKING', 'SUCCESS'),
(28, 27, 2400, '2023-12-23', 'WALLET', 'SUCCESS'),
(29, 28, 97618, '2024-01-05', 'UPI', 'SUCCESS'),
(30, 29, 26400, '2024-01-18', 'COD', 'SUCCESS'),
(31, 30, 2200, '2024-01-31', 'CARD', 'SUCCESS'),
(32, 31, 60860, '2024-02-13', 'UPI', 'SUCCESS'),
(33, 32, 27298, '2024-02-26', 'NETBANKING', 'SUCCESS'),
(34, 33, 32000, '2024-03-10', 'WALLET', 'SUCCESS'),
(35, 34, 22600, '2024-03-22', 'UPI', 'SUCCESS'),
(36, 34, 22600, '2024-03-26', 'UPI', 'REFUNDED'),
(37, 35, 71300, '2024-04-05', 'COD', 'SUCCESS'),
(38, 36, 1620, '2024-04-18', 'CARD', 'SUCCESS'),
(39, 37, 5097, '2024-05-01', 'UPI', 'SUCCESS'),
(40, 38, 88200, '2024-05-14', 'NETBANKING', 'SUCCESS'),
(41, 39, 3200, '2024-05-27', 'WALLET', 'SUCCESS'),
(42, 40, 105600, '2024-06-09', 'UPI', 'SUCCESS'),
(43, 41, 18600, '2024-06-22', 'COD', 'SUCCESS'),
(44, 42, 899, '2024-07-05', 'CARD', 'SUCCESS'),
(45, 43, 26550, '2024-07-18', 'UPI', 'SUCCESS'),
(46, 44, 31440, '2024-07-31', 'NETBANKING', 'SUCCESS'),
(47, 45, 28000, '2024-08-13', 'WALLET', 'SUCCESS'),
(48, 46, 21600, '2024-08-26', 'UPI', 'SUCCESS'),
(49, 47, 126600, '2024-09-08', 'COD', 'SUCCESS'),
(50, 48, 5850, '2024-09-21', 'CARD', 'SUCCESS');
INSERT INTO payments VALUES
(51, 49, 39000, '2024-10-04', 'UPI', 'SUCCESS'),
(52, 50, 24000, '2024-10-17', 'NETBANKING', 'SUCCESS');
