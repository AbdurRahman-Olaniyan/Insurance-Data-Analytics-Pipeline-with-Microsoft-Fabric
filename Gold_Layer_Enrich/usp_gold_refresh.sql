CREATE OR ALTER PROCEDURE gold.usp_refresh_gold
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunTimestamp DATETIME2(6) = SYSDATETIME();

    PRINT 'Starting Silver → Gold load...';

    EXEC gold.usp_load_gold;

    -- Dimension Logs

    INSERT INTO [gold].[load_log] (RunTimestamp, SourceLayer, TableName, [RowCount])
    SELECT @RunTimestamp, 'Silver', 'dim_customer', COUNT(*) FROM [gold].[dim_customer];

    INSERT INTO [gold].[load_log] (RunTimestamp, SourceLayer, TableName, [RowCount])
    SELECT @RunTimestamp, 'Silver', 'dim_agent', COUNT(*) FROM [gold].[dim_agent];

    INSERT INTO [gold].[load_log] (RunTimestamp, SourceLayer, TableName, [RowCount])
    SELECT @RunTimestamp, 'Silver', 'dim_product', COUNT(*) FROM [gold].[dim_product];

    INSERT INTO [gold].[load_log] (RunTimestamp, SourceLayer, TableName, [RowCount])
    SELECT @RunTimestamp, 'Silver', 'dim_date', COUNT(*) FROM [gold].[dim_date];


    -- Fact Logs

    INSERT INTO [gold].[load_log] (RunTimestamp, SourceLayer, TableName, [RowCount])
    SELECT @RunTimestamp, 'Silver', 'fact_policy', COUNT(*) FROM [gold].[fact_policy];

    INSERT INTO [gold].[load_log] (RunTimestamp, SourceLayer, TableName, [RowCount])
    SELECT @RunTimestamp, 'Silver', 'fact_transaction', COUNT(*) FROM [gold].[fact_transaction];

    INSERT INTO [gold].[load_log] (RunTimestamp, SourceLayer, TableName, [RowCount])
    SELECT @RunTimestamp, 'Silver', 'fact_insuranceclaim', COUNT(*) FROM [gold].[fact_insuranceclaim];

    INSERT INTO [gold].[load_log] (RunTimestamp, SourceLayer, TableName, [RowCount])
    SELECT @RunTimestamp, 'Silver', 'fact_incident', COUNT(*) FROM [gold].[fact_incident];

    INSERT INTO [gold].[load_log] (RunTimestamp, SourceLayer, TableName, [RowCount])
    SELECT @RunTimestamp, 'Silver', 'fact_renewal', COUNT(*) FROM [gold].[fact_renewal];

    PRINT 'Gold layer refresh completed successfully.';
END;
GO