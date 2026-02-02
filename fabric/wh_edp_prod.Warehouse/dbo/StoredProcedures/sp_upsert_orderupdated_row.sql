CREATE   PROCEDURE [dbo].[sp_upsert_orderupdated_row]
AS
BEGIN

    WITH base AS (
        SELECT 
            [bucket_bk],
            [row_bk],
            [orderNumber],
            [BucketNumber],
            [RowNumber],
            [SKU],
            [source_name],
            [deliveryStatus_value],
            [fraudStatus_value],
            [source_updated_datetime],
            [updated_datetime],
            [salesBrand],
            [customerNumber],
            [country],
            [currency],
            [status_value],
            [source_isExternal],
            [SingleSalesPrice],
            [ManualDiscount],
            [TotalDiscount],
            [TotalAmount],
            [VATRate],
            [orderDate],
            [OrderedQuantity],
            [DeliveredQuantity],
            [CancelledQuantity],
            [NotifiedReturnQuantity],
            [ReturnedQuantity],
            [SingleBasePrice],
            [Name],
            [Color],
            [Size],
            [Brand],
            [URL],
            [ImageName],
            CAST(orderDate AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time' AS datetime2(3)) AS orderDateLocal,
            ROW_NUMBER() OVER (PARTITION BY row_bk ORDER BY source_updated_datetime DESC) AS rn
        FROM "lh_edp_prod"."stream"."orderupdates_rowstream"
        --where source_updated_datetime > (select max(source_updated_datetime) from [wh_edp_prod].[dbo].[orderupdated_row])
        where source_updated_datetime > DATEADD(MINUTE, -15, (SELECT MAX(source_updated_datetime) FROM [wh_edp_prod].[dbo].[orderupdated_row]))
    ),
    dedup AS (
        SELECT * FROM base WHERE rn = 1
    )
    MERGE [wh_edp_prod].[dbo].[orderupdated_row] AS target
    USING dedup AS source
    ON target.[row_bk] = source.[row_bk]
    WHEN MATCHED THEN
        UPDATE SET
            target.[bucket_bk] = source.[bucket_bk],
            target.[orderNumber] = source.[orderNumber],
            target.[BucketNumber] = source.[BucketNumber],
            target.[RowNumber] = source.[RowNumber],
            target.[SKU] = source.[SKU],
            target.[rn] = source.[rn],
            target.[source_name] = source.[source_name],
            target.[deliveryStatus_value] = source.[deliveryStatus_value],
            target.[fraudStatus_value] = source.[fraudStatus_value],
            target.[source_updated_datetime] = source.[source_updated_datetime],
            target.[updated_datetime] = source.[updated_datetime],
            target.[salesBrand] = source.[salesBrand],
            target.[customerNumber] = source.[customerNumber],
            target.[country] = source.[country],
            target.[currency] = source.[currency],
            target.[status_value] = source.[status_value],
            target.[source_isExternal] = source.[source_isExternal],
            target.[SingleSalesPrice] = source.[SingleSalesPrice],
            target.[ManualDiscount] = source.[ManualDiscount],
            target.[TotalDiscount] = source.[TotalDiscount],
            target.[TotalAmount] = source.[TotalAmount],
            target.[VATRate] = source.[VATRate],
            target.[orderDate] = source.[orderDate],
            target.[OrderedQuantity] = source.[OrderedQuantity],
            target.[DeliveredQuantity] = source.[DeliveredQuantity],
            target.[CancelledQuantity] = source.[CancelledQuantity],
            target.[NotifiedReturnQuantity] = source.[NotifiedReturnQuantity],
            target.[ReturnedQuantity] = source.[ReturnedQuantity],
            target.[SingleBasePrice] = source.[SingleBasePrice],
            target.[Name] = source.[Name],
            target.[Color] = source.[Color],
            target.[Size] = source.[Size],
            target.[Brand] = source.[Brand],
            target.[URL] = source.[URL],
            target.[ImageName] = source.[ImageName],
            target.[orderDateLocal] = source.[orderDateLocal]
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ([bucket_bk], [row_bk], [orderNumber], [BucketNumber], [RowNumber], [SKU], [rn],
            [source_name], [deliveryStatus_value], [fraudStatus_value], [source_updated_datetime],
            [updated_datetime], [salesBrand], [customerNumber], [country], [currency], [status_value],
            [source_isExternal], [SingleSalesPrice], [ManualDiscount], [TotalDiscount], [TotalAmount],
            [VATRate], [orderDate], [OrderedQuantity], [DeliveredQuantity], [CancelledQuantity],
            [NotifiedReturnQuantity], [ReturnedQuantity], [SingleBasePrice], [Name], [Color], [Size],
            [Brand], [URL], [ImageName], [orderDateLocal])
        VALUES (source.[bucket_bk], source.[row_bk], source.[orderNumber], source.[BucketNumber],
            source.[RowNumber], source.[SKU], source.[rn], source.[source_name],
            source.[deliveryStatus_value], source.[fraudStatus_value], source.[source_updated_datetime],
            source.[updated_datetime], source.[salesBrand], source.[customerNumber], source.[country],
            source.[currency], source.[status_value], source.[source_isExternal], source.[SingleSalesPrice],
            source.[ManualDiscount], source.[TotalDiscount], source.[TotalAmount], source.[VATRate],
            source.[orderDate], source.[OrderedQuantity], source.[DeliveredQuantity], source.[CancelledQuantity],
            source.[NotifiedReturnQuantity], source.[ReturnedQuantity], source.[SingleBasePrice], source.[Name],
            source.[Color], source.[Size], source.[Brand], source.[URL], source.[ImageName], source.[orderDateLocal]);

END;