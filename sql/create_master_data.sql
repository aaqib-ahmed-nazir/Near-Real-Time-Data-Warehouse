-- Create database
CREATE DATABASE IF NOT EXISTS master_database;
USE master_database;

-- Drop customers table if it exists
DROP TABLE IF EXISTS customers;

-- Create customers table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255),
    gender VARCHAR(10)
);

-- Drop products table if it exists
DROP TABLE IF EXISTS products;

-- Create products table
CREATE TABLE products (
    productID INT PRIMARY KEY,
    productName VARCHAR(255),
    productPrice DECIMAL(10,2),
    supplierID INT,
    supplierName VARCHAR(255),
    storeID INT,
    storeName VARCHAR(255)
);
