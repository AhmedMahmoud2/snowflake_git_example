-- 01_setup.sql
-- Creates a dedicated database & schema for the demo.

-- Adjust names as you like (or comment out if you don't have privileges)
CREATE DATABASE IF NOT EXISTS NHS_DEMO;
CREATE SCHEMA IF NOT EXISTS NHS_DEMO.AE;

USE DATABASE NHS_DEMO;
USE SCHEMA AE;

-- Optional: warehouse selection (uncomment & change if needed)
-- USE WAREHOUSE <YOUR_WAREHOUSE>;
