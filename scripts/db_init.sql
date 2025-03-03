-- Run with caution, the script will delete the entire database if a database with the name warehouse exists.

-- Check if Database exists, Create warehouse database
DROP IF EXISTS warehouse;
CREATE DATABASE warehouse;

-- Create Database Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
