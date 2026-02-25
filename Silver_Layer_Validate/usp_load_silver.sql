CREATE OR ALTER PROCEDURE silver.usp_load_silver
AS
BEGIN

--incident
DROP TABLE IF EXISTS silver.incident;

CREATE TABLE silver.incident
AS 
SELECT
    NULLIF(LTRIM(RTRIM(IncidentID)), '') AS IncidentID,
    NULLIF(LTRIM(RTRIM(ClaimID)), '') AS ClaimID,
    TRY_CONVERT(date, NULLIF(LTRIM(RTRIM(IncidentDate)), ''), 103) AS IncidentDate,
    NULLIF(LTRIM(RTRIM(IncidentType)), '') AS IncidentType,
    NULLIF(LTRIM(RTRIM(Severity)), '') AS Severity,
    NULLIF(LTRIM(RTRIM([Description])), '') AS [Description]
FROM Insurance_Bronze.dbo.stg_incident
WHERE NULLIF(LTRIM(RTRIM(IncidentID)), '') IS NOT NULL;

--renewal
DROP TABLE IF EXISTS silver.renewal;

CREATE TABLE silver.renewal
AS
SELECT
    NULLIF(LTRIM(RTRIM(RenewalID)), '') AS RenewalID,
    NULLIF(LTRIM(RTRIM(PolicyNo)), '') AS PolicyNo,
    TRY_CONVERT(date, NULLIF(LTRIM(RTRIM(RenewalDate)), ''), 103) AS RenewalDate,
    TRY_CONVERT(decimal(18,2), REPLACE(NULLIF(LTRIM(RTRIM(Amount)), ''), ',', '')) AS Amount,
    NULLIF(LTRIM(RTRIM(PaymentMethod)), '') AS PaymentMethod
FROM Insurance_Bronze.dbo.stg_renewal
WHERE NULLIF(LTRIM(RTRIM(RenewalID)), '') IS NOT NULL;

--agent
DROP TABLE IF EXISTS silver.agent;

CREATE TABLE silver.agent
AS
SELECT 
    NULLIF(LTRIM(RTRIM(AgentID)),'') AS AgentID,
    NULLIF(LTRIM(RTRIM(AgentName)), '') AS AgentName,
    NULLIF(LTRIM(RTRIM(Region)), '') AS Region,
    TRY_CONVERT(date, NULLIF(LTRIM(RTRIM(HireDate)), ''), 103) AS HireDate,
    TRY_CONVERT(decimal(10,2), NULLIF(LTRIM(RTRIM(PerformanceRating)), '')) AS PerfomanceRating
FROM Insurance_Bronze.dbo.stg_agent
WHERE NULLIF(LTRIM(RTRIM(AgentID)), '') IS NOT NULL;

--customer
DROP TABLE IF EXISTS silver.customer

 CREATE TABLE silver.customer
 AS
 SELECT 
    NULLIF(LTRIM(RTRIM(CustomerID)), '') AS CustomerID,
    NULLIF(LTRIM(RTRIM(FullName)), '') AS FUllName,
    NULLIF(LTRIM(RTRIM(Gender)), '') AS Gender,
    TRY_CONVERT(date, NULLIF(LTRIM(RTRIM(DOB)), ''), 103) AS DOB,
    NULLIF(LTRIM(RTRIM(City)), '') AS City,
    NULLIF(LTRIM(RTRIM(Country)), '') AS Country,
    LOWER(NULLIF(LTRIM(RTRIM(Email)), '')) AS Email,
    NULLIF(LTRIM(RTRIM(Phone)), '') AS Phone,
    TRY_CONVERT(date, NULLIF(LTRIM(RTRIM(JoinDate)), ''), 103) AS JoinDate
FROM Insurance_Bronze.dbo.stg_customer
WHERE NULLIF(LTRIM(RTRIM(CustomerID)), '') IS NOT NULL;

--date
DROP TABLE IF EXISTS silver.dim_date

CREATE TABLE silver.dim_date
AS
SELECT 
    TRY_CONVERT(int, NULLIF(LTRIM(RTRIM(DateKey)), '')) AS DateKey,
    TRY_CONVERT(date, NULLIF(LTRIM(RTRIM(FullDate)), '')) AS FullDate,
    TRY_CONVERT(int, NULLIF(LTRIM(RTRIM([Year])), '')) AS [Year],
    NULLIF(LTRIM(RTRIM(Quarter)), '') AS Quarter,
    TRY_CONVERT(int, NULLIF(LTRIM(RTRIM([Month])), '')) AS [Month],
    NULLIF(LTRIM(RTRIM(MonthName)), '') AS MonthName,
    TRY_CONVERT(int, NULLIF(LTRIM(RTRIM([Day])), '')) AS [Day],
    NULLIF(LTRIM(RTRIM(DayOfWeek)), '') AS DayOfWeek
FROM Insurance_Bronze.dbo.stg_date
WHERE NULLIF(LTRIM(RTRIM(DateKey)), '') IS NOT NULL;

--product
DROP TABLE IF EXISTS silver.product;

CREATE TABLE silver.product
AS
SELECT 
    NULLIF(LTRIM(RTRIM(ProductID)), '') AS ProductID,
    NULLIF(LTRIM(RTRIM(ProductName)), '') AS ProductName,
    NULLIF(LTRIM(RTRIM(Category)), '') AS Category,
    TRY_CONVERT(decimal(18,2), REPLACE(NULLIF(LTRIM(RTRIM(Premium)), ''), ',', '')) AS Premium,
    NULLIF(LTRIM(RTRIM(CoverageType)), '') AS CoverageType,
    TRY_CONVERT(int, NULLIF(LTRIM(RTRIM(DurationMonths)), '')) AS DurationMonths
FROM Insurance_Bronze.dbo.stg_product
WHERE NULLIF(LTRIM(RTRIM(ProductID)), '') IS NOT NULL;

END;