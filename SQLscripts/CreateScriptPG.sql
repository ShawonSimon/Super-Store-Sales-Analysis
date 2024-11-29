 --create city dimension--
create table CityDimension(
    CityID SERIAL primary key,
    City VARCHAR(50),
    State VARCHAR(50),
    PostalCode INTEGER
);

 --create ship mode dimension--
create table ShipModeDimension(
    ShipModeId SERIAL primary key,
    ShipMode VARCHAR(15)
);

--create customer dimension--
create table CustomerDimension(
    CustomerID SERIAL primary key,
    CustomerCode VARCHAR (20) ,
    CustomerName VARCHAR(50),
    Segment VARCHAR(20)
);

--create product dimension--
CREATE TABLE ProductDimension (
    ProductID SERIAL PRIMARY KEY,
    ProductName VARCHAR(200) NOT NULL UNIQUE,
    Category VARCHAR(20),
    SubCategory VARCHAR(20)
);

--create date dimension--
create table DateDimension(
    DateID SERIAL primary key,
    OrderDate date,
    Weekday VARCHAR(10),
    month int,
    MonthName VARCHAR(10),
    Quarter int,
    year int,
    ShipDate date
);

--create fact table--
CREATE TABLE FactSales (
    OrderId SERIAL primary key,
    OrderCode VARCHAR(20) NOT NULL,
    DateID INT NOT NULL,
    ShipModeID INT NOT NULL,
    CustomerID INT NOT NULL,
    ProductID INT NOT NULL,
    CityID INT,
    Sales NUMERIC NOT NULL,
    FOREIGN KEY (DateID) REFERENCES DateDimension(DateID),
    FOREIGN KEY (ShipModeID) REFERENCES ShipModeDimension(ShipModeID),
    FOREIGN KEY (CustomerID) REFERENCES CustomerDimension(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES ProductDimension(ProductID),
    FOREIGN KEY (CityID) REFERENCES CityDimension(CityID)
);

--inserting data into city dimension--
insert into CityDimension (City, state, postalcode)
SELECT distinct
    city,
    state,
    CAST("Postal Code" AS numeric)
FROM superstoresales;

--inserting data into ship mode dimension--
insert into ShipModeDimension (ShipMode)
select distinct 
    "Ship Mode"
FROM superstoresales;

--inserting data into customer dimensionn--
insert into CustomerDimension (CustomerCode, CustomerName, Segment)
select distinct 
    "Customer ID",
    "Customer Name",
    segment
FROM superstoresales;

--inserting data into product dimensionn--
INSERT INTO ProductDimension (ProductName, Category, SubCategory)
SELECT DISTINCT
    "Product Name",
    Category,
    "Sub-Category"
FROM superstoresales;

--inserting data into date dimensionn--
INSERT INTO DateDimension (OrderDate, Weekday, month, MonthName, Quarter, year, ShipDate)
SELECT OrderDate, Weekday, month, MonthName, Quarter, year, ShipDate
FROM (
    SELECT DISTINCT 
        "Order Date" AS OrderDate,
        TRIM(TO_CHAR("Order Date", 'Day')) AS Weekday,
        EXTRACT(MONTH FROM "Order Date") AS month,
        TRIM(TO_CHAR("Order Date", 'Month')) AS MonthName,
        EXTRACT(QUARTER FROM "Order Date") AS Quarter,
        EXTRACT(YEAR FROM "Order Date") AS year,
        "Ship Date" as ShipDate,
        ROW_NUMBER() OVER (PARTITION BY "Order Date" ORDER BY "Order Date") AS rn
    FROM superstoresales
) sub
WHERE rn = 1;

--inserting data into fact table--
INSERT INTO FactSales (OrderCode, DateID, ShipModeID, CustomerID, ProductID, CityID, Sales)
SELECT 
    "Order ID" AS OrderCode,
    dd.DateID,
    smd.ShipModeID,
    cud.CustomerID,
    pd.ProductID,
    cd.CityID,
    Sales
FROM superstoresales ss
left join DateDimension dd on ss."Order Date" = dd.orderdate
left join ShipModeDimension smd on ss."Ship Mode" = smd.ShipMode
left join CustomerDimension cud on ss."Customer ID" = cud.customercode
left join ProductDimension pd on ss."Product Name" = pd.productname 
left join CityDimension cd on ss.City = cd.City AND ss.State = cd.State AND CAST(ss."Postal Code" AS numeric) = cd.PostalCode;