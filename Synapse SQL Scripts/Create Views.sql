-------------------------------
--Create Calendar View
-------------------------------
CREATE VIEW Gold.calendar
AS
SELECT * FROM 
OPENROWSET( BULK 'https://storageazureprojectaish.dfs.core.windows.net/silver/AdventureWorks_Calendar/',
            FORMAT= 'PARQUET')
            as query1

--------------------------------
--Create Customers View
-------------------------------
CREATE VIEW Gold.Customers
AS
SELECT * FROM 
OPENROWSET( BULK 'https://storageazureprojectaish.dfs.core.windows.net/silver/AdventureWorks_Customers/',
            FORMAT= 'PARQUET')
            as query1

 -------------------------------
--Create Product_Categories View
-------------------------------
CREATE VIEW Gold.Product_Categories
AS
SELECT * FROM 
OPENROWSET( BULK 'https://storageazureprojectaish.dfs.core.windows.net/silver/AdventureWorks_Product_Categories/',
            FORMAT= 'PARQUET')
            as query1

 -------------------------------
--Create Product_Subcategories View
-------------------------------
CREATE VIEW Gold.Product_Subcategories
AS
SELECT * FROM 
OPENROWSET( BULK 'https://storageazureprojectaish.dfs.core.windows.net/silver/AdventureWorks_Product_Subcategories/',
            FORMAT= 'PARQUET')
            as query1

-------------------------------
--Create Products View
-------------------------------
CREATE VIEW Gold.Products
AS
SELECT * FROM 
OPENROWSET( BULK 'https://storageazureprojectaish.dfs.core.windows.net/silver/AdventureWorks_Products/',
            FORMAT= 'PARQUET')
            as query1

 -------------------------------
--Create Returns View
-------------------------------
CREATE VIEW Gold.Returns
AS
SELECT * FROM 
OPENROWSET( BULK 'https://storageazureprojectaish.dfs.core.windows.net/silver/AdventureWorks_Returns/',
            FORMAT= 'PARQUET')
            as query1

-------------------------------
--Create Territories View
-------------------------------
CREATE VIEW Gold.Territories
AS
SELECT * FROM 
OPENROWSET( BULK 'https://storageazureprojectaish.dfs.core.windows.net/silver/AdventureWorks_Territories/',
            FORMAT= 'PARQUET')
            as query1

 -------------------------------
--Create Sales View
-------------------------------
CREATE VIEW Gold.Sales
AS
SELECT * FROM 
OPENROWSET( BULK 'https://storageazureprojectaish.dfs.core.windows.net/silver/AdventureWorks_Sales/',
            FORMAT= 'PARQUET')
            as query1                                                                               