CREATE     PROCEDURE [dbo].[sp_upsert_orderupdated_order]
AS
BEGIN
	WITH base AS (
		SELECT 
			[order_bk],
			[orderNumber],
			[status_value],
			[orderDate],
			[salesBrand],
			[country],
			[total_creditedAmount],
			[EventEnqueuedUtcTime],
			[updated_datetime],
			[invoiceAddress_address_countryCode],
			[total_handlingFee],
			[total_shippingFee],
			[total_salesAmount],
			[total_discount],
			[total_totalAmount],
			[invoiceAddress_lastName],
			[invoiceAddress_email],
			[invoiceAddress_mobilePhone],
			[invoiceAddress_address_street],
			[invoiceAddress_address_postalCode],
			[invoiceAddress_address_city],
			[fraudStatus_value],
			[fraudStatus_title],
			[deliveryStatus_value],
			[deliveryStatus_title],
			[customerNumber],
			[invoiceAddress_firstName],
			[currency],
			[source_name],
			[source_title],
			[source_isExternal],
			[clientIP],
			[status_title],
			ROW_NUMBER() OVER (PARTITION BY order_bk ORDER BY EventEnqueuedUtcTime DESC) AS rn
		FROM [lh_edp_prod].[stream].[orderupdates_orderstream]
		WHERE updated_datetime > DATEADD(MINUTE, -15 ,(SELECT MAX(updated_datetime) FROM [wh_edp_prod].[dbo].[orderupdated_order]))
	),
	dedup AS (
		SELECT * FROM base WHERE rn = 1
	)

	MERGE [wh_edp_prod].[dbo].[orderupdated_order] AS target
	USING dedup AS source
	ON target.[order_bk] = source.[order_bk]
	WHEN MATCHED AND target.EventEnqueuedUtcTime < source.EventEnqueuedUtcTime THEN
		UPDATE SET
			target.[orderNumber] = source.[orderNumber],
			target.[status_value] = source.[status_value],
			target.[orderDate] = source.[orderDate],
			target.[salesBrand] = source.[salesBrand],
			target.[country] = source.[country],
			target.[total_creditedAmount] = source.[total_creditedAmount],
			target.[EventEnqueuedUtcTime] = source.[EventEnqueuedUtcTime],
			target.[updated_datetime] = source.[updated_datetime],
			target.[invoiceAddress_address_countryCode] = source.[invoiceAddress_address_countryCode],
			target.[total_handlingFee] = source.[total_handlingFee],
			target.[total_shippingFee] = source.[total_shippingFee],
			target.[total_salesAmount] = source.[total_salesAmount],
			target.[total_discount] = source.[total_discount],
			target.[total_totalAmount] = source.[total_totalAmount],
			target.[invoiceAddress_lastName] = source.[invoiceAddress_lastName],
			target.[invoiceAddress_email] = source.[invoiceAddress_email],
			target.[invoiceAddress_mobilePhone] = source.[invoiceAddress_mobilePhone],
			target.[invoiceAddress_address_street] = source.[invoiceAddress_address_street],
			target.[invoiceAddress_address_postalCode] = source.[invoiceAddress_address_postalCode],
			target.[invoiceAddress_address_city] = source.[invoiceAddress_address_city],
			target.[fraudStatus_value] = source.[fraudStatus_value],
			target.[fraudStatus_title] = source.[fraudStatus_title],
			target.[deliveryStatus_value] = source.[deliveryStatus_value],
			target.[deliveryStatus_title] = source.[deliveryStatus_title],
			target.[customerNumber] = source.[customerNumber],
			target.[invoiceAddress_firstName] = source.[invoiceAddress_firstName],
			target.[currency] = source.[currency],
			target.[source_name] = source.[source_name],
			target.[source_title] = source.[source_title],
			target.[source_isExternal] = source.[source_isExternal],
			target.[clientIP] = source.[clientIP],
			target.[status_title] = source.[status_title],
			target.[rn] = source.[rn]
	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([order_bk], [orderNumber], [status_value], [orderDate], [salesBrand],
			[country], [total_creditedAmount], [EventEnqueuedUtcTime], [updated_datetime],
			[invoiceAddress_address_countryCode], [total_handlingFee], [total_shippingFee],
			[total_salesAmount], [total_discount], [total_totalAmount], [invoiceAddress_lastName],
			[invoiceAddress_email], [invoiceAddress_mobilePhone], [invoiceAddress_address_street],
			[invoiceAddress_address_postalCode], [invoiceAddress_address_city], [fraudStatus_value],
			[fraudStatus_title], [deliveryStatus_value], [deliveryStatus_title], [customerNumber],
			[invoiceAddress_firstName], [currency], [source_name], [source_title],
			[source_isExternal], [clientIP], [status_title], [rn])
		VALUES (source.[order_bk], source.[orderNumber], source.[status_value], source.[orderDate],
			source.[salesBrand], source.[country], source.[total_creditedAmount],
			source.[EventEnqueuedUtcTime], source.[updated_datetime],
			source.[invoiceAddress_address_countryCode], source.[total_handlingFee],
			source.[total_shippingFee], source.[total_salesAmount], source.[total_discount],
			source.[total_totalAmount], source.[invoiceAddress_lastName], source.[invoiceAddress_email],
			source.[invoiceAddress_mobilePhone], source.[invoiceAddress_address_street],
			source.[invoiceAddress_address_postalCode], source.[invoiceAddress_address_city],
			source.[fraudStatus_value], source.[fraudStatus_title], source.[deliveryStatus_value],
			source.[deliveryStatus_title], source.[customerNumber], source.[invoiceAddress_firstName],
			source.[currency], source.[source_name], source.[source_title], source.[source_isExternal],
			source.[clientIP], source.[status_title], source.[rn]);
END;