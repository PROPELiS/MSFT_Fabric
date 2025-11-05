CREATE   PROCEDURE [Propelis].[REFRESH_BILL_TO_CUST_GK]
    @MODE VARCHAR(10)
AS
BEGIN
    IF UPPER(@MODE) = 'FULL'
    BEGIN
        TRUNCATE TABLE [GLOBAL_EDW].[Propelis].[BILL_TO_CUST_GK];

        INSERT INTO [GLOBAL_EDW].[Propelis].[BILL_TO_CUST_GK]
        SELECT *
        FROM [GLOBAL_EDW].[Propelis].[BILL_TO_CUST];

        PRINT 'BILL_TO_CUST_GK TRUNCATED AND RELOADED SUCCESSFULLY!';
    END
    ELSE
    BEGIN
        MERGE [GLOBAL_EDW].[Propelis].[BILL_TO_CUST_GK] AS T
        USING [GLOBAL_EDW].[Propelis].[BILL_TO_CUST] AS S
        ON T.[CUST_KEY] = S.[CUST_KEY]

        WHEN MATCHED AND (
            ISNULL(T.[Bill To Customer ID],'') <> ISNULL(S.[Bill To Customer ID],'') OR
            ISNULL(T.[Bill To Customer Name],'') <> ISNULL(S.[Bill To Customer Name],'') OR
            ISNULL(T.[Bill To Cust Country Code],'') <> ISNULL(S.[Bill To Cust Country Code],'') OR
            ISNULL(T.[Bill To Cust City],'') <> ISNULL(S.[Bill To Cust City],'')
        )
        THEN UPDATE SET
            T.[Bill To Customer ID] = S.[Bill To Customer ID],
            T.[Bill To Customer Name] = S.[Bill To Customer Name],
            T.[Bill To Cust Country Code] = S.[Bill To Cust Country Code],
            T.[Bill To Cust City] = S.[Bill To Cust City]

        WHEN NOT MATCHED BY TARGET
        THEN INSERT (
            [CUST_KEY],
            [Bill To Customer ID],
            [Bill To Customer Name],
            [Bill To Cust Country Code],
            [Bill To Cust City]
        )
        VALUES (
            S.[CUST_KEY],
            S.[Bill To Customer ID],
            S.[Bill To Customer Name],
            S.[Bill To Cust Country Code],
            S.[Bill To Cust City]
        );

        PRINT 'BILL_TO_CUST_GK MERGED SUCCESSFULLY!';
    END
END;