 --create city dimension table--
create table City_Dimension(
    CityID SERIAL primary key,
    City VARCHAR(50),
    State VARCHAR(50),
    PostalCode INTEGER
);

 --create ship mode dimension table--
create table ShipMode_Dimension(
    ShipModeId SERIAL primary key,
    ShipMode VARCHAR(15)
);

--create customer dimension table--
create table Customer_Dimension(
    CustomerID SERIAL primary key,
    CustomerCode VARCHAR (20) ,
    CustomerName VARCHAR(50),
    Segment VARCHAR(20)
);

--create product dimension table--
CREATE TABLE Product_Dimension (
    ProductID SERIAL PRIMARY KEY,
    ProductName VARCHAR(200) NOT NULL UNIQUE,
    Category VARCHAR(20),
    SubCategory VARCHAR(20)
);

--create date dimension table--
create table Date_Dimension(
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
CREATE TABLE Fact_Sales (
    OrderId SERIAL primary key,
    OrderCode VARCHAR(20) NOT NULL,
    DateID INT NOT NULL,
    ShipModeID INT NOT NULL,
    CustomerID INT NOT NULL,
    ProductID INT NOT NULL,
    CityID INT,
    Sales NUMERIC NOT NULL,
    FOREIGN KEY (DateID) REFERENCES Date_Dimension(DateID),
    FOREIGN KEY (ShipModeID) REFERENCES ShipMode_Dimension(ShipModeID),
    FOREIGN KEY (CustomerID) REFERENCES Customer_Dimension(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES Product_Dimension(ProductID),
    FOREIGN KEY (CityID) REFERENCES City_Dimension(CityID)
);

--inserting data into city dimension--
insert into City_Dimension (City, state, postalcode)
SELECT distinct
    city,
    state,
    CAST("Postal Code" AS numeric)
FROM superstoresales;

--inserting data into ship mode dimension--
insert into ShipMode_Dimension (ShipMode)
select distinct 
    "Ship Mode"
FROM superstoresales;

--inserting data into customer dimensionn--
insert into Customer_Dimension (CustomerCode, CustomerName, Segment)
select distinct 
    "Customer ID",
    "Customer Name",
    segment
FROM superstoresales;

--inserting data into product dimensionn--
INSERT INTO Product_Dimension (ProductName, Category, SubCategory)
SELECT DISTINCT
    "Product Name",
    Category,
    "Sub-Category"
FROM superstoresales;

--inserting data into date dimensionn--
INSERT INTO Date_Dimension (OrderDate, Weekday, month, MonthName, Quarter, year, ShipDate)
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
INSERT INTO Fact_Sales (OrderCode, DateID, ShipModeID, CustomerID, ProductID, CityID, Sales)
SELECT 
    "Order ID" AS OrderCode,
    dd.DateID,
    smd.ShipModeID,
    cud.CustomerID,
    pd.ProductID,
    cd.CityID,
    Sales
FROM superstoresales ss
left join Date_Dimension dd on ss."Order Date" = dd.orderdate
left join ShipMode_Dimension smd on ss."Ship Mode" = smd.ShipMode
left join Customer_Dimension cud on ss."Customer ID" = cud.customercode
left join Product_Dimension pd on ss."Product Name" = pd.productname 
left join City_Dimension cd on ss.City = cd.City AND ss.State = cd.State AND CAST(ss."Postal Code" AS numeric) = cd.PostalCode;