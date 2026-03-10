CREATE OR ALTER PROCEDURE gold.usp_load_gold
AS
BEGIN
    SET NOCOUNT ON;

    -- gold.dim_customer
    TRUNCATE TABLE [gold].[dim_customer];

    INSERT INTO [gold].[dim_customer]
    (
        CustomerKey,
        CustomerID,
        FullName,
        Gender,
        DOB,
        City,
        Country,
        Email,
        Phone,
        JoinDate
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY CustomerID) AS CustomerKey,
        CustomerID,
        FullName,
        Gender,
        DOB,
        City,
        Country,
        Email,
        Phone,
        JoinDate
    FROM [Insurance_Silver].[silver].[customer];

    -- gold.dim_agent
    TRUNCATE TABLE [gold].[dim_agent];

    INSERT INTO [gold].[dim_agent]
    (
        AgentKey,
        AgentID,
        AgentName,
        Region,
        HireDate,
        PerformanceRating
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY AgentID) AS AgentKey,
        AgentID,
        AgentName,
        Region,
        HireDate,
        PerformanceRating
    FROM [Insurance_Silver].[silver].[agent];

    -- gold.dim_product
    TRUNCATE TABLE [gold].[dim_product];

    INSERT INTO [gold].[dim_product]
    (
        ProductKey,
        ProductID,
        ProductName,
        Category,
        Premium,
        CoverageType,
        DurationMonths
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY ProductID) AS ProductKey,
        ProductID,
        ProductName,
        Category,
        Premium,
        CoverageType,
        DurationMonths
    FROM [Insurance_Silver].[silver].[product];

    -- gold.dim_date
    TRUNCATE TABLE [gold].[dim_date];

    INSERT INTO [gold].[dim_date]
    (
        DateKey,
        FullDate,
        [Year],
        [Quarter],
        [Month],
        MonthName,
        [Day],
        DayOfWeek
    )
    SELECT
        DateKey,
        COALESCE(FullDate, CONVERT(date, CAST(DateKey AS varchar(8)))) AS FullDate,
        [Year],
        [Quarter],
        [Month],
        MonthName,
        [Day],
        DayOfWeek
    FROM [Insurance_Silver].[silver].[dim_date];


    -- gold.fact_policy
    TRUNCATE TABLE [gold].[fact_policy];

    INSERT INTO [gold].[fact_policy]
    (
        PolicyNo,
        CustomerKey,
        AgentKey,
        ProductKey,
        StartDateKey,
        EndDateKey,
        [Status]
    )
    SELECT
        p.PolicyNo,
        c.CustomerKey,
        a.AgentKey,
        pr.ProductKey,
        CAST(CONVERT(char(8), p.StartDate, 112) AS INT) AS StartDateKey,
        CAST(CONVERT(char(8), p.EndDate, 112) AS INT) AS EndDateKey,
        CASE 
            WHEN p.[Status] IS NOT NULL THEN p.[Status]
            WHEN EndDate < SYSDATETIME() THEN 'Expired'
            WHEN EndDate >= SYSDATETIME() THEN 'Active'
            ELSE 'Unknown'
        END AS PolicyStatus
    FROM [Insurance_Silver].[silver].[policy] p
    LEFT JOIN [gold].[dim_customer] c
        ON c.CustomerID = p.CustomerID
    LEFT JOIN [gold].[dim_agent] a
        ON a.AgentID = p.AgentID
    LEFT JOIN [gold].[dim_product] pr
        ON pr.ProductID = p.ProductID;


    -- gold.fact_transaction
    TRUNCATE TABLE [gold].[fact_transaction];

    INSERT INTO [gold].[fact_transaction]
    (
        TransactionID,
        PolicyNo,
        CustomerKey,
        AgentKey,
        ProductKey,
        DateKey,
        Amount,
        PaymentMethod,
        Channel
    )
    SELECT
        t.TransactionID,
        t.PolicyNo,
        c.CustomerKey,
        a.AgentKey,
        pr.ProductKey,
        CAST(CONVERT(char(8), t.TransactionDate, 112) AS INT) AS DateKey,
        t.Amount,
        t.PaymentMethod,
        ISNULL(t.Channel, 'Unknown') AS Channel
    FROM [Insurance_Silver].[silver].[transaction] t
    LEFT JOIN [Insurance_Silver].[silver].[policy] p
        ON p.PolicyNo = t.PolicyNo
    LEFT JOIN [gold].[dim_customer] c
        ON c.CustomerID = COALESCE(t.CustomerID, p.CustomerID)
    LEFT JOIN [gold].[dim_agent] a
        ON a.AgentID = p.AgentID
    LEFT JOIN [gold].[dim_product] pr
        ON pr.ProductID = COALESCE(t.ProductID, p.ProductID);


    -- gold.fact_insuranceclaim
    TRUNCATE TABLE [gold].[fact_insuranceclaim];

    INSERT INTO [gold].[fact_insuranceclaim]
    (
        ClaimID,
        PolicyNo,
        CustomerKey,
        AgentKey,
        ProductKey,
        DateKey,
        ClaimType,
        ClaimAmount,
        [Status],
        SettlementDateKey
    )
    SELECT
        c.ClaimID,
        c.PolicyNo,
        cu.CustomerKey,
        a.AgentKey,
        pr.ProductKey,
        CAST(CONVERT(char(8), c.ClaimDate, 112) AS INT) AS DateKey,
        c.ClaimType,
        CASE 
            WHEN c.Status = 'Approved' AND c.ClaimAmount IS NULL THEN 0.00   -- approved but missing amount
            WHEN c.Status IN ('Pending','Under Review') THEN NULL            -- keep null, means not yet settled
            WHEN c.Status = 'Rejected' THEN 0.00                             -- rejected claims have no payout
            ELSE c.ClaimAmount
        END AS ClaimAmount,
        c.[Status],
        CASE
            WHEN c.SettlementDate IS NULL THEN NULL
            ELSE CAST(CONVERT(char(8), c.SettlementDate, 112) AS INT)
        END AS SettlementDateKey
    FROM [Insurance_Silver].[silver].[insuranceclaim] c
    LEFT JOIN [Insurance_Silver].[silver].[policy] p
        ON p.PolicyNo = c.PolicyNo
    LEFT JOIN [gold].[dim_customer] cu
        ON cu.CustomerID = p.CustomerID
    LEFT JOIN [gold].[dim_agent] a
        ON a.AgentID = p.AgentID
    LEFT JOIN [gold].[dim_product] pr
        ON pr.ProductID = p.ProductID;


    -- gold.fact_incident
    TRUNCATE TABLE [gold].[fact_incident];

    INSERT INTO [gold].[fact_incident]
    (
        IncidentID,
        ClaimID,
        PolicyNo,
        CustomerKey,
        AgentKey,
        ProductKey,
        DateKey,
        IncidentType,
        Severity,
        [Description]
    )
    SELECT
        i.IncidentID,
        i.ClaimID,
        c.PolicyNo,
        cu.CustomerKey,
        a.AgentKey,
        pr.ProductKey,
        CAST(CONVERT(char(8), i.IncidentDate, 112) AS INT) AS DateKey,
        i.IncidentType,
        i.Severity,
        i.[Description]
    FROM [Insurance_Silver].[silver].[incident] i
    LEFT JOIN [Insurance_Silver].[silver].[insuranceclaim] c
        ON c.ClaimID = i.ClaimID
    LEFT JOIN [Insurance_Silver].[silver].[policy] p
        ON p.PolicyNo = c.PolicyNo
    LEFT JOIN [gold].[dim_customer] cu
        ON cu.CustomerID = p.CustomerID
    LEFT JOIN [gold].[dim_agent] a
        ON a.AgentID = p.AgentID
    LEFT JOIN [gold].[dim_product] pr
        ON pr.ProductID = p.ProductID;


    -- gold.fact_renewal
    TRUNCATE TABLE [gold].[fact_renewal];

    INSERT INTO [gold].[fact_renewal]
    (
        RenewalID,
        PolicyNo,
        CustomerKey,
        AgentKey,
        ProductKey,
        DateKey,
        Amount,
        PaymentMethod
    )
    SELECT
        r.RenewalID,
        r.PolicyNo,
        cu.CustomerKey,
        a.AgentKey,
        pr.ProductKey,
        CAST(CONVERT(char(8), r.RenewalDate, 112) AS INT) AS DateKey,
        r.Amount,
        r.PaymentMethod
    FROM [Insurance_Silver].[silver].[renewal] r
    LEFT JOIN [Insurance_Silver].[silver].[policy] p
        ON p.PolicyNo = r.PolicyNo
    LEFT JOIN [gold].[dim_customer] cu
        ON cu.CustomerID = p.CustomerID
    LEFT JOIN [gold].[dim_agent] a
        ON a.AgentID = p.AgentID
    LEFT JOIN [gold].[dim_product] pr
        ON pr.ProductID = p.ProductID;

END;
GO