-- Create a database master key --
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'MyPassword';

-- Create city dimension table --
CREATE TABLE [dbo].[City_Dimension]
(
    [CityId] [int] NOT NULL,
    [City] [NVARCHAR](20) NOT NULL,
    [State] [NVARCHAR](20) NOT NULL,
    [PostalCode] [NVARCHAR](10)
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

-- create ship mode dimension table --
CREATE TABLE [dbo].[ShipMode_Dimension] (
    [ShipModeId] [int] NOT NULL,
    [ShipMode] [NVARCHAR](15) NOT NULL
) WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

-- create customer dimension table --
CREATE TABLE [dbo].[Customer_Dimension] (
    [CustomerID] [int] NOT NULL,
    [CustomerCode] [NVARCHAR](20) NOT NULL,
    [CustomerName] [NVARCHAR](50) NOT NULL,
    [Segment] [NVARCHAR](20) NOT NULL
) WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

-- create product dimension table --
CREATE TABLE [dbo].[Product_Dimension] (
   [ProductID] [int] NOT NULL,
   [ProductName] [NVARCHAR](200) NOT NULL UNIQUE,
   [Category] [NVARCHAR](20) NULL,
   [SubCategory] [NVARCHAR](20) NULL
) WITH (
   DISTRIBUTION = REPLICATE,
   CLUSTERED COLUMNSTORE INDEX
);

-- create date dimension table --
CREATE TABLE [dbo].[Date_Dimension] (
   [DateID] [int] NOT NULL,
   [OrderDate] [date] NULL,
   [Weekday] [NVARCHAR](10) NULL,
   [Month] [int] NULL,
   [MonthName] [NVARCHAR](10) NULL,
   [Quarter] [int] NULL,
   [Year] [int] NULL,
   [ShipDate] [date] NULL
) WITH (
   DISTRIBUTION = REPLICATE,
   CLUSTERED COLUMNSTORE INDEX
);

-- create fact table --
CREATE TABLE [dbo].[Fact_Sales]
( 
	[OrderId] [int] NOT NULL,
    [OrderCode] [nvarchar](20)  NOT NULL,
	[DateId] [int]  NOT NULL,
	[ShipModeId] [int]  NOT NULL,
	[CustomerId] [int]  NOT NULL,
	[ProductId] [int]  NOT NULL,
	[CityId] [NVARCHAR](10),
	[SalesAmount] [money]  NOT NULL
)
WITH
(
	DISTRIBUTION = HASH ( [OrderId] ),
	CLUSTERED COLUMNSTORE INDEX
);

-- copy data into city dimension table --
COPY INTO [dbo].[City_Dimension]
FROM 'https://superstoresalessa.blob.core.windows.net/superstore/DimCity.csv'
WITH
(
    FILE_TYPE = 'CSV',
    CREDENTIAL = (IDENTITY = 'Managed Identity'),
    FIELDTERMINATOR = ',',
    FIELDQUOTE = '"',
    FIRSTROW = 2,
    ENCODING = 'UTF8'
);

-- copy data into ship mode dimension table --
COPY INTO [dbo].[Shipmode_Dimension]
FROM 'https://superstoresalessa.blob.core.windows.net/superstore/DimShipMode.csv'
WITH
(
    FILE_TYPE = 'CSV',
    CREDENTIAL = (IDENTITY = 'Managed Identity'),
    FIELDTERMINATOR = ',',
    FIELDQUOTE = '"',
    FIRSTROW = 2,
    ENCODING = 'UTF8'
);

-- copy data into customer dimension table --
COPY INTO [dbo].[Customer_Dimension]
FROM 'https://superstoresalessa.blob.core.windows.net/superstore/DimCustomer.csv'
WITH
(
    FILE_TYPE = 'CSV',
    CREDENTIAL = (IDENTITY = 'Managed Identity'),
    FIELDTERMINATOR = ',',
    FIELDQUOTE = '"',
    FIRSTROW = 2,
    ENCODING = 'UTF8'
);

-- copy data into product dimension table --
COPY INTO [dbo].[Product_Dimension]
FROM 'https://superstoresalessa.blob.core.windows.net/superstore/DimProduct.csv'
WITH
(
    FILE_TYPE = 'CSV',
    CREDENTIAL = (IDENTITY = 'Managed Identity'),
    FIELDTERMINATOR = ',',
    FIELDQUOTE = '"',
    FIRSTROW = 2,
    ENCODING = 'UTF8'
);

-- copy data into date dimension table --
COPY INTO [dbo].[Date_Dimension]
FROM 'https://superstoresalessa.blob.core.windows.net/superstore/DimDate.csv'
WITH
(
    FILE_TYPE = 'CSV',
    CREDENTIAL = (IDENTITY = 'Managed Identity'),
    FIELDTERMINATOR = ',',
    FIELDQUOTE = '"',
    FIRSTROW = 2,
    ENCODING = 'UTF8'
);

-- copy data into fact table table --
COPY INTO [dbo].[Fact_Sales]
FROM 'https://superstoresalessa.blob.core.windows.net/superstore/FactTable.csv'
WITH
(
    FILE_TYPE = 'CSV',
    CREDENTIAL = (IDENTITY = 'Managed Identity'),
    FIELDTERMINATOR = ',',
    FIELDQUOTE = '"',
    FIRSTROW = 2,
    ENCODING = 'UTF8'
);