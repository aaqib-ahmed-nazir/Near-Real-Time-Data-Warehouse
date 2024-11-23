SELECT DATABASE();
USE dw;

-- ============================================== Q1 ==============================================

SET @specified_year = 2019; 

WITH ProductSales AS (
    SELECT 
        DATE_FORMAT(dd.FULL_DATE, '%Y-%m') AS Month,
        dd.IS_WEEKEND,
        p.PRODUCT_NAME,
        SUM(ft.SALE_AMOUNT) as Total_Revenue
    FROM
        FACT_TRANSACTIONS ft
        JOIN PRODUCTS p ON ft.PRODUCT_ID = p.PRODUCT_ID
        JOIN DIM_DATE dd ON ft.DATE_ID = dd.DATE_ID
    WHERE
        dd.YEAR = @specified_year 
    GROUP BY 
        DATE_FORMAT(dd.FULL_DATE, '%Y-%m'),
        dd.IS_WEEKEND,
        p.PRODUCT_NAME
),
RankedProducts AS (
    SELECT
        Month,
        CASE WHEN IS_WEEKEND = 1 THEN 'Weekend' ELSE 'Weekday' END AS Day_Type,
        PRODUCT_NAME,
        Total_Revenue,
        ROW_NUMBER() OVER (
            PARTITION BY Month, IS_WEEKEND
            ORDER BY Total_Revenue DESC
        ) AS rnk
    FROM
        ProductSales
),
FinalSelection AS (
    SELECT
        Month,
        Day_Type,
        PRODUCT_NAME,
        Total_Revenue
    FROM
        RankedProducts
    WHERE
        rnk <= 5
)
SELECT
    Month,
    Day_Type,
    PRODUCT_NAME,
    FORMAT(Total_Revenue, 2) as Revenue
FROM
    FinalSelection
ORDER BY
    Total_Revenue DESC;

-- ============================================== Q1 ==============================================

-- -- ============================================== Q2 ==============================================

-- WITH QuarterlyRevenue AS (
--     SELECT 
--         s.STORE_ID,
--         s.STORE_NAME,
--         d.YEAR,
--         d.QUARTER,
--         SUM(ft.SALE_AMOUNT) AS QUARTERLY_REVENUE
--     FROM FACT_TRANSACTIONS ft
--     JOIN STORES s ON ft.STORE_ID = s.STORE_ID
--     JOIN DIM_DATE d ON ft.DATE_ID = d.DATE_ID
--     WHERE d.YEAR = 2019
--     GROUP BY s.STORE_ID, s.STORE_NAME, d.YEAR, d.QUARTER
-- ),
-- GrowthRate AS (
--     SELECT 
--         STORE_ID,
--         STORE_NAME,
--         QUARTER,
--         QUARTERLY_REVENUE,
--         LAG(QUARTERLY_REVENUE) OVER (PARTITION BY STORE_ID ORDER BY QUARTER) AS PREV_QUARTER_REVENUE,
--         CASE 
--             WHEN LAG(QUARTERLY_REVENUE) OVER (PARTITION BY STORE_ID ORDER BY QUARTER) IS NULL THEN NULL
--             WHEN LAG(QUARTERLY_REVENUE) OVER (PARTITION BY STORE_ID ORDER BY QUARTER) = 0 THEN NULL
--             ELSE ROUND(((QUARTERLY_REVENUE - LAG(QUARTERLY_REVENUE) OVER (PARTITION BY STORE_ID ORDER BY QUARTER)) / 
--                         LAG(QUARTERLY_REVENUE) OVER (PARTITION BY STORE_ID ORDER BY QUARTER)) * 100, 2)
--         END AS GROWTH_RATE
--     FROM QuarterlyRevenue
-- )
-- SELECT 
--     STORE_NAME,
--     QUARTER,
--     ROUND(QUARTERLY_REVENUE, 2) AS QUARTERLY_REVENUE,
--     GROWTH_RATE
-- FROM GrowthRate
-- ORDER BY STORE_ID, QUARTER;

-- -- ============================================== Q2 ==============================================

-- -- ============================================== Q3 ==============================================

-- WITH StoreSales AS (
--     SELECT 
--         s.STORE_ID,
--         s.STORE_NAME,
--         sup.SUPPLIER_NAME,
--         p.PRODUCT_NAME,
--         SUM(ft.SALE_AMOUNT) as Total_Sales,
--         SUM(SUM(ft.SALE_AMOUNT)) OVER (PARTITION BY s.STORE_NAME) as Store_Total_Sales
--     FROM
--         FACT_TRANSACTIONS ft
--         JOIN STORES s ON ft.STORE_ID = s.STORE_ID
--         JOIN PRODUCTS p ON ft.PRODUCT_ID = p.PRODUCT_ID
--         JOIN SUPPLIERS sup ON p.SUPPLIER_ID = sup.SUPPLIER_ID
--     GROUP BY
--         s.STORE_ID,
--         s.STORE_NAME,
--         sup.SUPPLIER_NAME,
--         p.PRODUCT_NAME
-- )
-- SELECT 
--     STORE_NAME,
--     SUPPLIER_NAME,
--     PRODUCT_NAME,
--     FORMAT(Total_Sales, 2) as Sales_Amount,
--     CONCAT(FORMAT((Total_Sales / Store_Total_Sales * 100), 2), '%') as Sales_Contribution
-- FROM 
--     StoreSales
-- ORDER BY
--     Total_Sales DESC; 

-- -- ============================================== Q3 ==============================================

-- -- ============================================== Q4 ==============================================

-- WITH SeasonalAnalysis AS (
--     SELECT 
--         dd.SEASON,
--         p.PRODUCT_NAME,
--         SUM(ft.SALE_AMOUNT) as Sales_Amount
--     FROM
--         FACT_TRANSACTIONS ft
--         JOIN PRODUCTS p ON ft.PRODUCT_ID = p.PRODUCT_ID
--         JOIN DIM_DATE dd ON ft.DATE_ID = dd.DATE_ID
--     GROUP BY 
--         dd.SEASON,
--         p.PRODUCT_NAME
--     WITH ROLLUP
-- )
-- SELECT 
--     CASE
--         WHEN SEASON IS NULL AND PRODUCT_NAME IS NULL THEN 'Grand Total'
--         WHEN SEASON IS NULL THEN 'Subtotal'
--         ELSE SEASON
--     END as Season,
--     COALESCE(PRODUCT_NAME, 'All Products') as Product,
--     FORMAT(Sales_Amount, 2) as Total_Sales
-- FROM 
--     SeasonalAnalysis
-- WHERE 
--     SEASON IN ('Spring', 'Summer', 'Fall', 'Winter')
--     OR SEASON IS NULL
-- ORDER BY
--     CASE 
--         WHEN SEASON IS NULL AND PRODUCT_NAME IS NULL THEN 0
--         WHEN SEASON IS NULL THEN 1
--         ELSE FIELD(SEASON, 'Spring', 'Summer', 'Fall', 'Winter')
--     END,
--     CASE WHEN PRODUCT_NAME IS NULL THEN 0 ELSE 1 END,
--     Sales_Amount DESC;

-- -- ============================================== Q4 ==============================================

-- -- ============================================== Q5 ==============================================

-- WITH MonthlyRevenue AS (
--     SELECT 
--         ft.STORE_ID,
--         s.STORE_NAME,
--         p.SUPPLIER_ID,
--         sp.SUPPLIER_NAME,
--         YEAR(d.FULL_DATE) AS YEAR,
--         MONTH(d.FULL_DATE) AS MONTH,
--         SUM(ft.SALE_AMOUNT) AS TOTAL_REVENUE
--     FROM FACT_TRANSACTIONS ft
--     JOIN STORES s ON ft.STORE_ID = s.STORE_ID
--     JOIN PRODUCTS p ON ft.PRODUCT_ID = p.PRODUCT_ID
--     JOIN SUPPLIERS sp ON p.SUPPLIER_ID = sp.SUPPLIER_ID
--     JOIN DIM_DATE d ON ft.DATE_ID = d.DATE_ID
--     GROUP BY 
--         ft.STORE_ID, s.STORE_NAME, 
--         p.SUPPLIER_ID, sp.SUPPLIER_NAME, 
--         YEAR(d.FULL_DATE), MONTH(d.FULL_DATE)
-- ),
-- RevenueVolatility AS (
--     SELECT
--         STORE_ID,
--         STORE_NAME,
--         SUPPLIER_ID,
--         SUPPLIER_NAME,
--         YEAR,
--         MONTH,
--         TOTAL_REVENUE,
--         LAG(TOTAL_REVENUE) OVER (
--             PARTITION BY STORE_ID, SUPPLIER_ID 
--             ORDER BY YEAR, MONTH
--         ) AS PREV_MONTH_REVENUE,
--         CASE
--             WHEN LAG(TOTAL_REVENUE) OVER (
--                 PARTITION BY STORE_ID, SUPPLIER_ID 
--                 ORDER BY YEAR, MONTH
--             ) IS NULL THEN NULL
--             WHEN LAG(TOTAL_REVENUE) OVER (
--                 PARTITION BY STORE_ID, SUPPLIER_ID 
--                 ORDER BY YEAR, MONTH
--             ) = 0 THEN NULL
--             ELSE ROUND(
--                 ((TOTAL_REVENUE - LAG(TOTAL_REVENUE) OVER (
--                     PARTITION BY STORE_ID, SUPPLIER_ID 
--                     ORDER BY YEAR, MONTH
--                 )) / LAG(TOTAL_REVENUE) OVER (
--                     PARTITION BY STORE_ID, SUPPLIER_ID 
--                     ORDER BY YEAR, MONTH
--                 )) * 100, 2)
--         END AS VOLATILITY_PERCENT
--     FROM MonthlyRevenue
-- )
-- SELECT
--     STORE_NAME,
--     SUPPLIER_NAME,
--     YEAR,
--     MONTH,
--     TOTAL_REVENUE,
--     VOLATILITY_PERCENT
-- FROM RevenueVolatility
-- ORDER BY 
--     STORE_ID, SUPPLIER_ID, YEAR, MONTH;

-- -- ============================================== Q5 ==============================================

-- -- ============================================== Q6 ==============================================

-- WITH OrderProducts AS (
--     SELECT
--         ORDER_ID,
--         PRODUCT_ID
--     FROM FACT_TRANSACTIONS
-- ),

-- ProductPairs AS (
--     SELECT
--         op1.PRODUCT_ID AS PRODUCT_ID_1,
--         op2.PRODUCT_ID AS PRODUCT_ID_2,
--         COUNT(DISTINCT op1.ORDER_ID) AS TOGETHER_COUNT
--     FROM OrderProducts op1
--     JOIN OrderProducts op2 ON op1.ORDER_ID = op2.ORDER_ID AND op1.PRODUCT_ID < op2.PRODUCT_ID
--     GROUP BY op1.PRODUCT_ID, op2.PRODUCT_ID
-- ),

-- TopProductPairs AS (
--     SELECT
--         PRODUCT_ID_1,
--         PRODUCT_ID_2,
--         TOGETHER_COUNT
--     FROM ProductPairs
--     ORDER BY TOGETHER_COUNT DESC
--     LIMIT 5
-- )

-- SELECT
--     p1.PRODUCT_NAME AS PRODUCT_NAME_1,
--     p2.PRODUCT_NAME AS PRODUCT_NAME_2,
--     tp.TOGETHER_COUNT
-- FROM TopProductPairs tp
-- JOIN PRODUCTS p1 ON tp.PRODUCT_ID_1 = p1.PRODUCT_ID
-- JOIN PRODUCTS p2 ON tp.PRODUCT_ID_2 = p2.PRODUCT_ID
-- ORDER BY tp.TOGETHER_COUNT DESC;

-- -- =============================================== Q6 ==============================================

-- -- =============================================== Q7 ==============================================
-- SELECT
--     d.YEAR,
--     s.STORE_NAME,
--     sp.SUPPLIER_NAME,
--     p.PRODUCT_NAME,
--     SUM(ft.SALE_AMOUNT) AS TOTAL_REVENUE
-- FROM FACT_TRANSACTIONS ft
-- JOIN STORES s ON ft.STORE_ID = s.STORE_ID
-- JOIN PRODUCTS p ON ft.PRODUCT_ID = p.PRODUCT_ID
-- JOIN SUPPLIERS sp ON p.SUPPLIER_ID = sp.SUPPLIER_ID
-- JOIN DIM_DATE d ON ft.DATE_ID = d.DATE_ID
-- GROUP BY d.YEAR, s.STORE_NAME, sp.SUPPLIER_NAME, p.PRODUCT_NAME WITH ROLLUP
-- ORDER BY d.YEAR, s.STORE_NAME, sp.SUPPLIER_NAME, p.PRODUCT_NAME;

-- -- ============================================== Q7 ==============================================

-- -- ============================================== Q8 ==============================================

-- SELECT
--     p.PRODUCT_ID,
--     p.PRODUCT_NAME,
--     -- Revenue
--     SUM(CASE WHEN MONTH(d.FULL_DATE) BETWEEN 1 AND 6 THEN ft.SALE_AMOUNT ELSE 0 END) AS H1_REVENUE,
--     SUM(CASE WHEN MONTH(d.FULL_DATE) BETWEEN 7 AND 12 THEN ft.SALE_AMOUNT ELSE 0 END) AS H2_REVENUE,
--     SUM(ft.SALE_AMOUNT) AS TOTAL_YEAR_REVENUE,
--     -- Quantity
--     SUM(CASE WHEN MONTH(d.FULL_DATE) BETWEEN 1 AND 6 THEN ft.QUANTITY ELSE 0 END) AS H1_QUANTITY,
--     SUM(CASE WHEN MONTH(d.FULL_DATE) BETWEEN 7 AND 12 THEN ft.QUANTITY ELSE 0 END) AS H2_QUANTITY,
--     SUM(ft.QUANTITY) AS TOTAL_YEAR_QUANTITY
-- FROM FACT_TRANSACTIONS ft
-- JOIN PRODUCTS p ON ft.PRODUCT_ID = p.PRODUCT_ID
-- JOIN DIM_DATE d ON ft.DATE_ID = d.DATE_ID
-- WHERE d.YEAR = 2019  
-- GROUP BY p.PRODUCT_ID, p.PRODUCT_NAME
-- ORDER BY p.PRODUCT_ID;

-- -- ================================================= Q8 =============================================

-- -- ================================================= Q9 =============================================

-- WITH DailySales AS (
--     SELECT
--         p.PRODUCT_ID,
--         p.PRODUCT_NAME,
--         d.FULL_DATE,
--         SUM(ft.SALE_AMOUNT) AS DAILY_SALES
--     FROM FACT_TRANSACTIONS ft
--     JOIN PRODUCTS p ON ft.PRODUCT_ID = p.PRODUCT_ID
--     JOIN DIM_DATE d ON ft.DATE_ID = d.DATE_ID
--     GROUP BY p.PRODUCT_ID, p.PRODUCT_NAME, d.FULL_DATE
-- ),

-- AverageDailySales AS (
--     SELECT
--         PRODUCT_ID,
--         PRODUCT_NAME,
--         AVG(DAILY_SALES) AS AVG_DAILY_SALES
--     FROM DailySales
--     GROUP BY PRODUCT_ID, PRODUCT_NAME
-- ),

-- SalesSpikes AS (
--     SELECT
--         ds.PRODUCT_ID,
--         ds.PRODUCT_NAME,
--         ds.FULL_DATE,
--         ds.DAILY_SALES,
--         ads.AVG_DAILY_SALES,
--         CASE
--             WHEN ds.DAILY_SALES > 2 * ads.AVG_DAILY_SALES THEN 'Sales Spike'
--             ELSE 'Normal'
--         END AS SALES_SPIKE
--     FROM DailySales ds
--     JOIN AverageDailySales ads ON ds.PRODUCT_ID = ads.PRODUCT_ID
-- )

-- SELECT
--     PRODUCT_ID,
--     PRODUCT_NAME,
--     FULL_DATE,
--     DAILY_SALES,
--     AVG_DAILY_SALES,
--     SALES_SPIKE
-- FROM SalesSpikes
-- WHERE SALES_SPIKE = 'Outlier'
-- ORDER BY PRODUCT_ID, FULL_DATE;

-- -- ============================================== Q9 ===============================================

-- -- ============================================== Q10 ==============================================

-- -- Drop the view if it exists
-- DROP VIEW IF EXISTS STORE_QUARTERLY_SALES;

-- CREATE VIEW STORE_QUARTERLY_SALES AS
-- SELECT
--     s.STORE_ID,
--     s.STORE_NAME,
--     d.YEAR,
--     d.QUARTER,
--     SUM(ft.SALE_AMOUNT) AS TOTAL_QUARTERLY_SALES
-- FROM FACT_TRANSACTIONS ft
-- JOIN STORES s ON ft.STORE_ID = s.STORE_ID
-- JOIN DIM_DATE d ON ft.DATE_ID = d.DATE_ID
-- GROUP BY s.STORE_ID, s.STORE_NAME, d.YEAR, d.QUARTER
-- ORDER BY s.STORE_NAME, d.YEAR, d.QUARTER;

-- -- Testing the view
-- SELECT * FROM STORE_QUARTERLY_SALES;

-- -- ============================================= Q10 ================================================


