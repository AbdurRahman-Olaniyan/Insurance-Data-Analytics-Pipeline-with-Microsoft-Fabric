-- Create schema if it does not already exist
CREATE SCHEMA [gold];
GO

-- create dimension tables
CREATE TABLE [gold].[dim_customer]
(
    [CustomerKey] BIGINT,
    [CustomerID] VARCHAR(50),
    [FullName] VARCHAR(200),
    [Gender] VARCHAR(50),
    [DOB] DATE,
    [City] VARCHAR(100),
    [Country] VARCHAR(100),
    [Email] VARCHAR(200),
    [Phone] VARCHAR(50),
    [JoinDate] DATE
);
GO

CREATE TABLE [gold].[dim_agent]
(
    [AgentKey] BIGINT,
    [AgentID] VARCHAR(50),
    [AgentName] VARCHAR(200),
    [Region] VARCHAR(100),
    [HireDate] DATE,
    [PerformanceRating] DECIMAL(10,2)
);
GO

CREATE TABLE [gold].[dim_product]
(
    [ProductKey] BIGINT,
    [ProductID] VARCHAR(50),
    [ProductName] VARCHAR(200),
    [Category] VARCHAR(100),
    [Premium] DECIMAL(18,2),
    [CoverageType] VARCHAR(100),
    [DurationMonths] INT
);
GO

CREATE TABLE [gold].[dim_date]
(
    [DateKey] INT,
    [FullDate] DATE,
    [Year] INT,
    [Quarter] VARCHAR(10),
    [Month] INT,
    [MonthName] VARCHAR(20),
    [Day] INT,
    [DayOfWeek] VARCHAR(20)
);
GO

-- create fact tables
CREATE TABLE [gold].[fact_policy]
(
    [PolicyNo] VARCHAR(50),
    [CustomerKey] BIGINT,
    [AgentKey] BIGINT,
    [ProductKey] BIGINT,
    [StartDateKey] INT,
    [EndDateKey] INT,
    [Status] VARCHAR(50)
);
GO

CREATE TABLE [gold].[fact_transaction]
(
    [TransactionID] VARCHAR(50),
    [PolicyNo] VARCHAR(50),
    [CustomerKey] BIGINT,
    [AgentKey] BIGINT,
    [ProductKey] BIGINT,
    [DateKey] INT,
    [Amount] DECIMAL(18,2),
    [PaymentMethod] VARCHAR(100),
    [Channel] VARCHAR(100)
);
GO

CREATE TABLE [gold].[fact_insuranceclaim]
(
    [ClaimID] VARCHAR(50),
    [PolicyNo] VARCHAR(50),
    [CustomerKey] BIGINT,
    [AgentKey] BIGINT,
    [ProductKey] BIGINT,
    [DateKey] INT,
    [ClaimType] VARCHAR(100),
    [ClaimAmount] DECIMAL(18,2),
    [Status] VARCHAR(50),
    [SettlementDateKey] INT
);
GO

CREATE TABLE [gold].[fact_incident]
(
    [IncidentID] VARCHAR(50),
    [ClaimID] VARCHAR(50),
    [PolicyNo] VARCHAR(50),
    [CustomerKey] BIGINT,
    [AgentKey] BIGINT,
    [ProductKey] BIGINT,
    [DateKey] INT,
    [IncidentType] VARCHAR(100),
    [Severity] VARCHAR(50),
    [Description] VARCHAR(1000)
);
GO

CREATE TABLE [gold].[fact_renewal]
(
    [RenewalID] VARCHAR(50),
    [PolicyNo] VARCHAR(50),
    [CustomerKey] BIGINT,
    [AgentKey] BIGINT,
    [ProductKey] BIGINT,
    [DateKey] INT,
    [Amount] DECIMAL(18,2),
    [PaymentMethod] VARCHAR(100)
);
GO

CREATE TABLE [gold].[load_log]
(
    [RunTimestamp] DATETIME2(6),
    [SourceLayer] VARCHAR(50),
    [TableName] VARCHAR(100),
    [RowCount] BIGINT,
    [MissingDimensionKeys] INT NULL,
    [InvalidDates] INT NULL
);
GO