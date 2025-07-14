CREATE DATABASE SCOPED CREDENTIAL cred_rathna
WITH
    IDENTITY = 'Managed Identity'


CREATE EXTERNAL DATA SOURCE source_silver
WITH
(
    LOCATION = 'https://awstoragedl15.dfs.core.windows.net/silver',
    CREDENTIAL = cred_rathna
)

CREATE EXTERNAL DATA SOURCE source_gold
WITH
(
    LOCATION = 'https://awstoragedl15.dfs.core.windows.net/gold',
    CREDENTIAL = cred_rathna
)

CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) as SELECT * FROM gold.sales

SELECT * FROM gold.extsales