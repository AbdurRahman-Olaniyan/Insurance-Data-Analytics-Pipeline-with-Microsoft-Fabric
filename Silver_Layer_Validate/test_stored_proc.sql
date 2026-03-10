CREATE OR ALTER PROCEDURE silver.usp_refresh_silver
AS
BEGIN
    SET NOCOUNT ON;

    -- Use DATETIME2(6) for precision down to microseconds
    DECLARE @RunTimestamp DATETIME2(6) = SYSDATETIME();

    PRINT 'Starting Bronze → Silver load...';
    EXEC silver.usp_load_silver;

    -- Log Bronze tables
    INSERT INTO silver.load_log (RunTimestamp, SourceType, TableName, [RowCount])
    SELECT @RunTimestamp, 'Bronze', 'incident', COUNT(*) FROM silver.incident;

    INSERT INTO silver.load_log (RunTimestamp, SourceType, TableName, [RowCount])
    SELECT @RunTimestamp, 'Bronze', 'renewal', COUNT(*) FROM silver.renewal;

    INSERT INTO silver.load_log (RunTimestamp, SourceType, TableName, [RowCount])
    SELECT @RunTimestamp, 'Bronze', 'agent', COUNT(*) FROM silver.agent;

    INSERT INTO silver.load_log (RunTimestamp, SourceType, TableName, [RowCount])
    SELECT @RunTimestamp, 'Bronze', 'customer', COUNT(*) FROM silver.customer;

    INSERT INTO silver.load_log (RunTimestamp, SourceType, TableName, [RowCount])
    SELECT @RunTimestamp, 'Bronze', 'dim_date', COUNT(*) FROM silver.dim_date;

    INSERT INTO silver.load_log (RunTimestamp, SourceType, TableName, [RowCount])
    SELECT @RunTimestamp, 'Bronze', 'product', COUNT(*) FROM silver.product;

    PRINT 'Starting Mirror → Silver load...';
    EXEC silver.usp_load_silver_from_mirror;

    -- Log Mirror tables with anomaly checks
    INSERT INTO silver.load_log (RunTimestamp, SourceType, TableName, [RowCount], MissingAmounts, InvalidDates)
    SELECT @RunTimestamp, 'Mirror', 'insuranceclaim',
           COUNT(*),
           SUM(CASE WHEN ClaimAmount IS NULL THEN 1 ELSE 0 END),
           SUM(CASE WHEN ClaimDate IS NULL OR (SettlementDate IS NULL AND Status = 'Approved') THEN 1 ELSE 0 END)
    FROM silver.insuranceclaim;

    INSERT INTO silver.load_log (RunTimestamp, SourceType, TableName, [RowCount], NegativeAmounts, InvalidDates)
    SELECT @RunTimestamp, 'Mirror', 'transaction',
           COUNT(*),
           SUM(CASE WHEN Amount < 0 THEN 1 ELSE 0 END),
           SUM(CASE WHEN TransactionDate IS NULL THEN 1 ELSE 0 END)
    FROM silver.[transaction];

    INSERT INTO silver.load_log (RunTimestamp, SourceType, TableName, [RowCount], InvalidDates)
    SELECT @RunTimestamp, 'Mirror', 'policy',
           COUNT(*),
           SUM(CASE WHEN StartDate IS NULL OR EndDate IS NULL OR StartDate > EndDate THEN 1 ELSE 0 END)
    FROM silver.policy;

    PRINT 'Silver layer refresh completed successfully.';
END;
GO

-- Test the stored procedure
-- EXEC silver.usp_refresh_silver;