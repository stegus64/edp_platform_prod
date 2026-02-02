CREATE   PROCEDURE [dbo].[sp_upsert_orderupdated_payment]
AS
BEGIN
	WITH base AS (
		SELECT 
			[pspReference],
			[EventEnqueuedUtcTime],
			[updated_datetime],
			[status_value],
			[title],
			[psp],
			[method],
			[alternative],
			[amount],
			[order_bk],
			[payment_bk],
			[orderNumber],
			ROW_NUMBER() OVER (PARTITION BY payment_bk ORDER BY EventEnqueuedUtcTime DESC) AS rn
		FROM [lh_edp_prod].[stream].[orderupdates_paymentstream]
		WHERE EventEnqueuedUtcTime > DATEADD(MINUTE, -15, (SELECT MAX(EventEnqueuedUtcTime) FROM [wh_edp_prod].[dbo].[orderupdated_payment]))
	),
	dedup AS (
		SELECT * FROM base WHERE rn = 1
	)

	MERGE [wh_edp_prod].[dbo].[orderupdated_payment] AS target
	USING dedup AS source
	ON target.[payment_bk] = source.[payment_bk]
	WHEN MATCHED THEN
		UPDATE SET
			target.[pspReference] = source.[pspReference],
			target.[EventEnqueuedUtcTime] = source.[EventEnqueuedUtcTime],
			target.[updated_datetime] = source.[updated_datetime],
			target.[status_value] = source.[status_value],
			target.[title] = source.[title],
			target.[psp] = source.[psp],
			target.[method] = source.[method],
			target.[alternative] = source.[alternative],
			target.[amount] = source.[amount],
			target.[order_bk] = source.[order_bk],
			target.[orderNumber] = source.[orderNumber],
			target.[rn] = source.[rn]
	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([pspReference], [EventEnqueuedUtcTime], [updated_datetime], [status_value],
			[title], [psp], [method], [alternative], [amount], [order_bk], [payment_bk],
			[orderNumber], [rn])
		VALUES (source.[pspReference], source.[EventEnqueuedUtcTime], source.[updated_datetime],
			source.[status_value], source.[title], source.[psp], source.[method],
			source.[alternative], source.[amount], source.[order_bk], source.[payment_bk],
			source.[orderNumber], source.[rn]);
END;