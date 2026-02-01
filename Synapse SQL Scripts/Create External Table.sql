CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Projectaish@25'


--CREATING CREDENTIALS
CREATE DATABASE SCOPED CREDENTIAL cred_aish
WITH IDENTITY= 'Managed Identity'


--CREATING EXTERNAL DATA SOURCE(FOR BOTH SILVER AND GOLD)
CREATE EXTERNAL DATA SOURCE source_silver
with
(LOCATION= 'https://storageazureprojectaish.dfs.core.windows.net/silver'--till container level
,CREDENTIAL = cred_aish
)

--CREATING EXTERNAL DATA SOURCE(GOLD)
CREATE EXTERNAL DATA SOURCE source_gold
with
(LOCATION= 'https://storageazureprojectaish.dfs.core.windows.net/gold'--till container level
,CREDENTIAL = cred_aish)

CREATE EXTERNAL FILE FORMAT Format_Parquet
WITH(FORMAT_TYPE= PARQUET,DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec')


---CREATE EXTERNAL TABLE EXTSALES

CREATE EXTERNAL TABLE gold.extsales
WITH    
    (LOCATION= 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS 
SELECT * from gold.sales --table created for sales earlier

--Querying the ext sales table
SELECT * from gold.extsales 


-----Next Table
---CREATE EXTERNAL TABLE EXTSALES

CREATE EXTERNAL TABLE gold.extcustomer
WITH    
    (LOCATION= 'extcustomer',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS 
SELECT * from gold.customers --table created for customer earlier

--Querying the ext sales table
SELECT * from gold.extcustomer 

-----Next Table
---CREATE EXTERNAL TABLE EXTSALES

CREATE EXTERNAL TABLE gold.extcalendar
WITH    
    (LOCATION= 'extcalendar',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS 
SELECT * from gold.calendar --table created for cucalendarstomer earlier

--Querying the ext sales table
SELECT * from gold.extcalendar 

-----Next Table
---CREATE EXTERNAL TABLE EXTSALES

CREATE EXTERNAL TABLE gold.extProduct_Categories
WITH    
    (LOCATION= 'extProduct_Categories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS 
SELECT * from gold.Product_Categories --table created for cucalendarstomer earlier

--Querying the ext sales table
SELECT * from gold.extProduct_Categories 

-----Next Table
---CREATE EXTERNAL TABLE EXTSALES

CREATE EXTERNAL TABLE gold.extProduct_Subcategories
WITH    
    (LOCATION= 'extProduct_Subcategories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS 
SELECT * from gold.Product_Subcategories --table created for cucalendarstomer earlier

--Querying the ext sales table
SELECT * from gold.extProduct_Subcategories 

-----Next Table
---CREATE EXTERNAL TABLE EXTSALES

CREATE EXTERNAL TABLE gold.extProducts
WITH    
    (LOCATION= 'extProduct',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS 
SELECT * from gold.Products --table created for cucalendarstomer earlier

--Querying the ext sales table
SELECT * from gold.extProducts

-----Next Table
---CREATE EXTERNAL TABLE EXTSALES

CREATE EXTERNAL TABLE gold.extReturns
WITH    
    (LOCATION= 'extReturns',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS 
SELECT * from gold.Returns --table created for cucalendarstomer earlier

--Querying the ext sales table
SELECT * from gold.extReturns

-------Next Table
---CREATE EXTERNAL TABLE EXTSALES

CREATE EXTERNAL TABLE gold.extTerritories
WITH    
    (LOCATION= 'extTerritories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) 
AS 
SELECT * from gold.Territories --table created for cucalendarstomer earlier

--Querying the ext sales table
SELECT * from gold.extTerritories