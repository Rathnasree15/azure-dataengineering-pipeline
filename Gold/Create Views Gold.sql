-- Calendar
CREATE OR ALTER VIEW gold.calendar AS
SELECT *
FROM OPENROWSET(
    BULK 'https://awstoragedl15.dfs.core.windows.net/silver/AdventureWorks_Calendar/',
    FORMAT = 'PARQUET'
) AS calendar;

-- Customers
CREATE OR ALTER VIEW gold.customers AS
SELECT *
FROM OPENROWSET(
    BULK 'https://awstoragedl15.dfs.core.windows.net/silver/AdventureWorks_Customers/',
    FORMAT = 'PARQUET'
) AS customers;

-- Products
CREATE OR ALTER VIEW gold.products AS
SELECT *
FROM OPENROWSET(
    BULK 'https://awstoragedl15.dfs.core.windows.net/silver/AdventureWorks_Products/',
    FORMAT = 'PARQUET'
) AS products;

-- Returns
CREATE OR ALTER VIEW gold.returns AS
SELECT *
FROM OPENROWSET(
    BULK 'https://awstoragedl15.dfs.core.windows.net/silver/AdventureWorks_Returns/',
    FORMAT = 'PARQUET'
) AS returns;

-- Sales
CREATE OR ALTER VIEW gold.sales AS
SELECT *
FROM OPENROWSET(
    BULK 'https://awstoragedl15.dfs.core.windows.net/silver/AdventureWorks_Sales/',
    FORMAT = 'PARQUET'
) AS sales;

-- Subcategories
CREATE OR ALTER VIEW gold.subcat AS
SELECT *
FROM OPENROWSET(
    BULK 'https://awstoragedl15.dfs.core.windows.net/silver/AdventureWorks_Product_Subcategories/',
    FORMAT = 'PARQUET'
) AS subcat;

-- Territories
CREATE OR ALTER VIEW gold.territories AS
SELECT *
FROM OPENROWSET(
    BULK 'https://awstoragedl15.dfs.core.windows.net/silver/AdventureWorks_Territories/',
    FORMAT = 'PARQUET'
) AS territories;
