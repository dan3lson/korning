DROP TABLE IF EXISTS
--   employees,
--   customers,
--   products,
   sales;
--   invoice_frequencies;

CREATE TABLE employees (
  id SERIAL PRIMARY KEY,
  first_name VARCHAR(255),
  last_name VARCHAR(255),
  email VARCHAR(255)
);

CREATE TABLE customers (
  id SERIAL PRIMARY KEY,
  company VARCHAR(255),
  account_number VARCHAR(255)
);

CREATE TABLE products (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255)
);

CREATE TABLE sales (
  id SERIAL PRIMARY KEY,
  day VARCHAR(255),
  amount VARCHAR(255),
  units INTEGER,
  invoice_number INTEGER,
  employee_id INTEGER,
  customer_id INTEGER,
  product_id INTEGER,
  invoice_frequency_id INTEGER
);

CREATE TABLE invoice_frequencies (
  id SERIAL PRIMARY KEY,
  frequency VARCHAR(255)
);
