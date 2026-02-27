CREATE OR ALTER PROCEDURE silver.usp_load_silver_from_mirror
AS
BEGIN
    SET NOCOUNT ON;

    -- policy
    DROP TABLE IF EXISTS silver.policy;

    CREATE TABLE silver.policy
    AS
    SELECT
        NULLIF(LTRIM(RTRIM(PolicyNo)), '') AS PolicyNo,
        NULLIF(LTRIM(RTRIM(CustomerID)), '') AS CustomerID,
        NULLIF(LTRIM(RTRIM(ProductID)), '') AS ProductID,
        NULLIF(LTRIM(RTRIM(AgentID)), '') AS AgentID,
        StartDate,
        EndDate,
        NULLIF(LTRIM(RTRIM(Status)), '') AS Status
    FROM InsuranceDB.dbo.policy
    WHERE NULLIF(LTRIM(RTRIM(PolicyNo)), '') IS NOT NULL;

    -- insuranceclaim
    DROP TABLE IF EXISTS silver.insuranceclaim;

    CREATE TABLE silver.insuranceclaim
    AS
    SELECT
        NULLIF(LTRIM(RTRIM(ClaimID)), '') AS ClaimID,
        NULLIF(LTRIM(RTRIM(PolicyNo)), '') AS PolicyNo,
        ClaimDate,
        NULLIF(LTRIM(RTRIM(ClaimType)), '') AS ClaimType,
        TRY_CONVERT(decimal(18,2), 
            REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(ClaimAmount)), ''), ',', ''), '₦', '')) AS ClaimAmount,
        NULLIF(LTRIM(RTRIM(Status)), '') AS Status,
        SettlementDate
    FROM InsuranceDB.dbo.insuranceclaim
    WHERE NULLIF(LTRIM(RTRIM(ClaimID)), '') IS NOT NULL;

    -- transaction
    DROP TABLE IF EXISTS silver.[transaction];

    CREATE TABLE silver.[transaction]
    AS
    SELECT
        NULLIF(LTRIM(RTRIM(TransactionID)), '') AS TransactionID,
        NULLIF(LTRIM(RTRIM(PolicyNo)), '') AS PolicyNo,
        NULLIF(LTRIM(RTRIM(CustomerID)), '') AS CustomerID,
        NULLIF(LTRIM(RTRIM(ProductID)), '') AS ProductID,
        TransactionDate,
        TRY_CONVERT(decimal(18,2), 
            REPLACE(REPLACE(NULLIF(LTRIM(RTRIM(Amount)), ''), ',', ''), '₦', '')) AS Amount,
        NULLIF(LTRIM(RTRIM(PaymentMethod)), '') AS PaymentMethod,
        NULLIF(LTRIM(RTRIM(Channel)), '') AS Channel
    FROM InsuranceDB.dbo.[transaction]
    WHERE NULLIF(LTRIM(RTRIM(TransactionID)), '') IS NOT NULL;

END;