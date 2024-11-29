-- 1. Customer Segmentation Analysis
WITH CustomerMetrics AS (
    SELECT 
        cd.segment,
        COUNT cd.customerid as total_customers,
        SUM(f.SalesAmount) as total_revenue,
        COUNT f.orderid as total_orders
    FROM dbo.Customer_Dimension cd
    JOIN dbo.Fact_Sales f ON cd.customerid = f.customerid
    GROUP BY cd.segment
)
SELECT 
    segment,
    total_customers,
    total_revenue,
    total_orders,
    total_revenue/total_customers as avg_revenue_per_customer,
    total_orders/total_customers as avg_orders_per_customer,
    total_revenue/total_orders as avg_order_value
FROM CustomerMetrics
ORDER BY total_revenue DESC;

-- 2. Top 10 Customers by Revenue
SELECT TOP 10
    cd.customername,
    cd.segment,
    COUNT f.orderid as total_orders,
    SUM(f.SalesAmount) as total_revenue,
    SUM(f.SalesAmount)/COUNT f.orderid as avg_order_value,
    MIN(d.orderdate) as first_purchase,
    MAX(d.orderdate) as last_purchase,
    DATEDIFF(day, MIN(d.orderdate), MAX(d.orderdate)) as days_as_customer
FROM dbo.Customer_Dimension cd
JOIN dbo.Fact_Sales f ON cd.customerid = f.customerid
JOIN dbo.Date_Dimension d ON f.dateid = d.dateid
GROUP BY cd.customername, cd.segment
ORDER BY total_revenue DESC;

-- 3. Day of Week Performance
SELECT 
    d.weekday,
    COUNT f.orderid as number_of_orders,
    SUM(f.salesamount) as total_sales,
    AVG(f.salesamount) as avg_daily_sales,
    COUNT cd.customerid as number_of_customers
FROM dbo.Fact_Sales f
JOIN dbo.Date_Dimension d ON f.dateid = d.dateid
JOIN dbo.Customer_Dimension cd ON f.customerid = cd.customerid
GROUP BY d.weekday
ORDER BY total_sales DESC;

-- 4. Quarterly Performance by Product Category
SELECT 
    d.year,
    d.quarter,
    pd.category,
    SUM(f.salesamount) as quarterly_sales,
    COUNT f.orderid as number_of_orders,
    COUNT cd.CustomerID as number_of_customers,
    SUM(f.salesamount)/COUNT f.orderid as avg_order_value
FROM dbo.Fact_Sales f
JOIN date_dimension d ON f.dateid = d.dateid
JOIN dbo.Product_Dimension pd ON f.productid = pd.ProductID
JOIN dbo.Customer_Dimension cd on f.CustomerId = cd.CustomerId
GROUP BY d.year, d.quarter, pd.category
ORDER BY d.year, d.quarter, quarterly_sales DESC;

-- 5. Peak Sales Days Analysis
SELECT TOP 10
    d.orderdate,
    d.weekday,
    COUNT f.orderid as number_of_orders,
    SUM(f.salesamount) as daily_sales,
    COUNT cd.CustomerID as unique_customers
FROM dbo.Fact_Sales f
JOIN dbo.Date_Dimension d ON f.dateid = d.dateid
JOIN dbo.Customer_Dimension cd ON f.CustomerId = cd.CustomerId
GROUP BY d.orderdate, d.weekday
ORDER BY daily_sales DESC;
