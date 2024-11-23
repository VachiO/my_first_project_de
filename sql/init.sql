-- Create the datawarehouse database
CREATE DATABASE datawarehouse;

-- Connect to the datawarehouse database
\c datawarehouse;

-- Create the random_users table
CREATE TABLE IF NOT EXISTS random_users (
    id SERIAL PRIMARY KEY,
    gender TEXT,
    name TEXT,
    email TEXT
);
