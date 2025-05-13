CREATE DATABASE STOCK_4_COM_DB;
USE DATABASE STOCK_4_COM_DB;
CREATE SCHEMA STOCK;
USE SCHEMA STOCK;

CREATE OR REPLACE TABLE STOCKS_OF_FOUR_COMPANIES(
    Date DATE,
    Close DOUBLE,
    High DOUBLE,
    Low DOUBLE,
    Open DOUBLE,
    Volume BIGINT,
    symbol varchar,
    close_change float,
    close_pct_change float
);

-- should be empty
SELECT * FROM STOCKS_OF_FOUR_COMPANIES;

CREATE OR REPLACE FILE FORMAT PIP_FORMAT_COMMA -- Because the file in comma
	type = 'CSV'
	field_delimiter = ','
	skip_header = 1;

-- storage integration
CREATE OR REPLACE STORAGE INTEGRATION S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = '--------'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('-----------');


-- valdating integration
DESC INTEGRATION S3_INTEGRATION;

-- creating stage
CREATE OR REPLACE STAGE S3_INTEGRATEION_BULK_COPY_4_C_STOCKS
  STORAGE_INTEGRATION = S3_INTEGRATION
  URL = '------------------'
  FILE_FORMAT = (format_name = PIP_FORMAT_COMMA);

-- validating integration
LIST @S3_INTEGRATEION_BULK_COPY_4_C_STOCKS;

-- Need to give the snowflake ARN & ID

-- Copy data using integration
COPY INTO STOCKS_OF_FOUR_COMPANIES FROM @S3_INTEGRATEION_BULK_COPY_4_C_STOCKS;

-- data should be there
SELECT * FROM STOCKS_OF_FOUR_COMPANIES;

select symbol, sum(volume) from stocks_of_four_companies
group by symbol;

select 
    symbol,
    max(close_pct_change)
from stocks_of_four_companies
group by symbol;

