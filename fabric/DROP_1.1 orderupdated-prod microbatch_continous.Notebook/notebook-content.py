# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "408d8412-c973-46aa-8c0c-52dbe1c38ada",
# META       "default_lakehouse_name": "lh_edp_prod",
# META       "default_lakehouse_workspace_id": "fb736e26-5db5-424b-8125-6f2ab29a83f0",
# META       "known_lakehouses": [
# META         {
# META           "id": "408d8412-c973-46aa-8c0c-52dbe1c38ada"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# # OrderUpdated - Micro Batch

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import * #col, concat_ws, md5, current_timestamp, trim, lower
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from notebookutils import mssparkutils
from datetime import datetime
import uuid
import time


spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------
# Notebook Parameter
# ---------------------------
try:
    from notebookutils import mssparkutils
    mode = mssparkutils.widgets.get("mode")
    if not mode:
        mode = "monitor"
except:
    mode = "monitor"

print(f"Mode = {mode}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Configuration

# CELL ********************

# ---------------------------
# Configuration
# ---------------------------
eventstream_table = "lh_edp_prod.stream.orderupdated_raw"
checkpoint_base = "Files/checkpoints/orderupdated_"
checkpoint_name = "multi_silver"


silver_tables = {
    "order":     {"table": "orderupdated_order",     "keys": ["order_hashkey"]},
    "payment":   {"table": "orderupdated_payment",   "keys": ["payment_hashkey"]},
    "offerhead": {"table": "orderupdated_offerhead", "keys": ["offerhead_hashkey"]},
    "bucket":    {"table": "orderupdated_bucket",    "keys": ["bucket_hashkey"]},
    "row":       {"table": "orderupdated_row",       "keys": ["row_hashkey"]},
    "offerline": {"table": "orderupdated_offerline", "keys": ["offerline_hashkey"]}
}


# Lock configuration
lock_table = "stream.orderupdated_locks"
lock_name = "multi_silver_batch"
lock_ttl = 30


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Preprocess Raw

# CELL ********************

# ---------------------------
# Preprocess Bronze
# ---------------------------
import pyspark.sql.functions as F

def preprocess_bronze(df):


    # ✅ Convert to timestamp
    #df = df.withColumn("orderDate", F.to_timestamp("orderDate"))
    df = df.withColumn("orderDate",from_utc_timestamp(col("orderDate"), "Europe/Stockholm"))

    # Optional: parse nested JSON field if needed
    status_schema = StructType([
        StructField("value", StringType(), True),
        StructField("title", StringType(), True)
    ])

    df = df.withColumn("status_parsed", from_json(col("status"), status_schema)) \
                        .withColumn("status_value", col("status_parsed.value")) \
                        .withColumn("status_title", col("status_parsed.title"))
    df = df.withColumn("updated_datetime", current_timestamp())

    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Transform Functions

# MARKDOWN ********************

# ##### Transform Order

# CELL ********************

def transform_order(df):

    print(f"{datetime.now()} ℹ️ transform_order")
    
    # Define schemas for nested JSON columns
    source_schema = StructType([
        StructField("name", StringType()),
        StructField("title", StringType()),
        StructField("isExternal", BooleanType())
    ])

    fraud_schema = StructType([
        StructField("value", StringType()),
        StructField("title", StringType())
    ])

    delivery_schema = StructType([
        StructField("value", StringType()),
        StructField("title", StringType())
    ])

    address_schema = StructType([
        StructField("street", StringType()),
        StructField("postalCode", StringType()),
        StructField("city", StringType()),
        StructField("countryCode", StringType())
    ])

    invoice_schema = StructType([
        StructField("firstName", StringType()),
        StructField("lastName", StringType()),
        StructField("address", address_schema),
        StructField("email", StringType()),
        StructField("mobilePhone", StringType())
    ])

    total_schema = StructType([
        StructField("handlingFee", DoubleType()),
        StructField("shippingFee", DoubleType()),
        StructField("salesAmount", DoubleType()),
        StructField("discount", DoubleType()),
        StructField("totalAmount", DoubleType()),
        StructField("creditedAmount", DoubleType())
    ])

    # Parse JSON columns into structs
    df_parsed = (
        df.withColumn("source", F.from_json("source", source_schema))
        .withColumn("fraudStatus", F.from_json("fraudStatus", fraud_schema))
        .withColumn("deliveryStatus", F.from_json("deliveryStatus", delivery_schema))
        .withColumn("invoiceAddress", F.from_json("invoiceAddress", invoice_schema))
        .withColumn("total", F.from_json("total", total_schema))
        .withColumn("order_bk", concat_ws("_", col("orderNumber"), col("status_value")))
        .withColumn("order_hashkey", md5(concat_ws("_", col("orderNumber"), col("status_value"))))
    )

    # Flatten fields
    df_order = df_parsed.select(
        col("order_bk").alias("order_bk"),
        col("order_hashkey").alias("order_hashkey"),

        col("orderNumber").alias("orderNumber"),
        col("status_value").alias("status_value"),
        col("orderDate").alias("orderDate"),
        col("salesBrand").alias("salesBrand"),
        col("country").alias("country"),
        col("currency").alias("currency"),

        col("source.name").alias("source_name"),
        col("source.title").alias("source_title"),
        col("source.isExternal").alias("source_isExternal"),

        col("clientIP").alias("clientIP"),
        col("status_title").alias("status_title"),

        col("fraudStatus.value").alias("fraudStatus_value"),
        col("fraudStatus.title").alias("fraudStatus_title"),

        col("deliveryStatus.value").alias("deliveryStatus_value"),
        col("deliveryStatus.title").alias("deliveryStatus_title"),

        col("customerNumber").alias("customerNumber"),

        col("invoiceAddress.firstName").alias("invoiceAddress_firstName"),
        col("invoiceAddress.lastName").alias("invoiceAddress_lastName"),
        col("invoiceAddress.email").alias("invoiceAddress_email"),
        col("invoiceAddress.mobilePhone").alias("invoiceAddress_mobilePhone"),
        col("invoiceAddress.address.street").alias("invoiceAddress_address_street"),
        col("invoiceAddress.address.postalCode").alias("invoiceAddress_address_postalCode"),
        col("invoiceAddress.address.city").alias("invoiceAddress_address_city"),
        col("invoiceAddress.address.countryCode").alias("invoiceAddress_address_countryCode"),

        col("total.handlingFee").alias("total_handlingFee"),
        col("total.shippingFee").alias("total_shippingFee"),
        col("total.salesAmount").alias("total_salesAmount"),
        col("total.discount").alias("total_discount"),
        col("total.totalAmount").alias("total_totalAmount"),
        col("total.creditedAmount").alias("total_creditedAmount"),
        col("updated_datetime").alias("updated_datetime")
    )

    return df_order


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Transform Payment

# CELL ********************

def transform_payment(df):

    print(f"{datetime.now()} ℹ️ transform_payment")

    payment_schema = ArrayType(
        StructType([
            StructField("title", StringType(), True),
            StructField("psp", StringType(), True),
            StructField("method", StringType(), True),
            StructField("alternative", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("pspReference", StringType(), True),
        ])
    )

    # Parse JSON into array of structs
    df_parsed = df.withColumn("payments", F.from_json(F.col("payments"), payment_schema))

    # Explode array to rows
    df_exploded = df_parsed.withColumn("payment", F.explode("payments"))
    #Replace NULL with "" (nested)      -- Not nested syntax: df = df.withColumn("myColumn", coalesce(col("myColumn"), lit("")))
    df_exploded = (
            df_exploded.withColumn("payment", col("payment").withField("pspReference", coalesce(col("payment.pspReference"), lit(""))))
            .withColumn("payment", col("payment").withField("alternative", coalesce(col("payment.alternative"), lit(""))))
    )
    # Add key
    df_exploded = (
        df_exploded.withColumn("payment_bk", concat_ws("_", col("orderNumber"), col("status_value"), col("payment.title"), col("payment.psp"), col("payment.method"), col("payment.pspReference")))
            .withColumn("payment_hashkey",  md5(concat_ws("_", col("orderNumber"), col("status_value"), col("payment.title"), col("payment.psp"), col("payment.method"), col("payment.pspReference"))))
    )

    df_payment = df_exploded.select(
        col("payment_bk").alias("payment_bk"),
        col("payment_hashkey").alias("payment_hashkey"),
        col("orderNumber").alias("orderNumber"),
        col("status_value").alias("status_value"),
        col("payment.title").alias("title"),
        col("payment.psp").alias("psp"),
        col("payment.method").alias("method"),
        col("payment.alternative").alias("alternative"),
        col("payment.amount").alias("amount"),
        col("payment.pspReference").alias("pspReference"),
        col("updated_datetime").alias("updated_datetime")
    )

    return df_payment


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Transform OfferHead

# CELL ********************

def transform_offerhead(df):

    print(f"{datetime.now()} ℹ️ transform_offerhead")

    # Define schema of offers JSON
    offers_schema = ArrayType(
        StructType([
            StructField("campaignId", StringType(), True),
            StructField("title", StringType(), True),
            StructField("activationCode", StringType(), True),
            StructField("discount", DoubleType(), True),
        ])
    )

    # Parse JSON into array<struct>
    df_parsed = df.withColumn("offers", F.from_json("offers", offers_schema))

    # Explode array to rows (one row per offer)
    df_exploded = df_parsed.withColumn("offer", F.explode("offers"))

    # Flatten fields
    df_flattened = (
        df_exploded
        .withColumn("campaignId", F.col("offer.campaignId"))
        .withColumn("title", F.col("offer.title"))
        .withColumn("activationCode", F.col("offer.activationCode"))
        .withColumn("discount", F.col("offer.discount"))
        .withColumn("offerhead_bk",       concat_ws("_", col("orderNumber"), col("status_value"), col("offer.campaignId"), col("offer.activationCode")))
        .withColumn("offerhead_hashkey", md5(concat_ws("_", col("orderNumber"), col("status_value"), col("offer.campaignId"), col("offer.activationCode"))))
        .drop("offer", "offers")
    )


    df_offerhead = df_flattened.select(
        col("offerhead_bk").alias("offerhead_bk"),
        col("offerhead_hashkey").alias("offerhead_hashkey"),
        col("orderNumber").alias("orderNumber"),
        col("status_value").alias("status_value"),
        col("campaignId").alias("campaignId"),
        col("title").alias("title"),
        col("activationCode").alias("activationCode"),
        col("discount").alias("discount"),
        col("updated_datetime").alias("updated_datetime")
    )
    
    return df_offerhead


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Transform Bucket

# CELL ********************

def transform_bucket(df):

    print(f"{datetime.now()} ℹ️ transform_bucket")

    # Step 1: Define the nested schema
    row_schema = StructType([
        StructField("rowNumber", IntegerType(), True),
        StructField("sku", StringType(), True),
        StructField("name", StringType(), True),
        StructField("color", StringType(), True),
        StructField("size", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("url", StringType(), True),
        StructField("imageName", StringType(), True),
        StructField("orderedQuantity", IntegerType(), True),
        StructField("deliveredQuantity", IntegerType(), True),
        StructField("cancelledQuantity", IntegerType(), True),
        StructField("notifiedReturnQuantity", IntegerType(), True),
        StructField("returnedQuantity", IntegerType(), True),
        StructField("singleBasePrice", DoubleType(), True),
        StructField("singleSalesPrice", DoubleType(), True),
        StructField("offers", ArrayType(StructType([
            StructField("campaignId", StringType(), True),
            StructField("discount", DoubleType(), True)
        ])), True),
        StructField("manualDiscount", DoubleType(), True),
        StructField("totalDiscount", DoubleType(), True),
        StructField("totalAmount", DoubleType(), True),
        StructField("vatRate", DoubleType(), True)
    ])

    bucket_schema = ArrayType(StructType([
        StructField("bucketNumber", IntegerType(), True),
        StructField("returnCode", StringType(), True),
        StructField("deliveredFrom", StructType([
            StructField("name", StringType(), True)
        ]), True),
        StructField("deliveryAddress", StructType([
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("address", StructType([
                StructField("street", StringType(), True),
                StructField("postalCode", StringType(), True),
                StructField("city", StringType(), True),
                StructField("countryCode", StringType(), True)
            ]), True),
            StructField("email", StringType(), True),
            StructField("mobilePhone", StringType(), True)
        ]), True),
        StructField("pickupPoint", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("address", StructType([
                StructField("street", StringType(), True),
                StructField("postalCode", StringType(), True),
                StructField("city", StringType(), True),
                StructField("countryCode", StringType(), True)
            ]), True)
        ]), True),
        StructField("deliveryEstimate", StructType([
            StructField("date", StringType(), True),
            StructField("text", StringType(), True)
        ]), True),
        StructField("shipping", StructType([
            StructField("carrier", StructType([
                StructField("code", StringType(), True),
                StructField("name", StringType(), True)
            ]), True),
            StructField("method", StringType(), True),
            StructField("title", StringType(), True),
            StructField("fee", DoubleType(), True),
            StructField("returnFee", DoubleType(), True),
            StructField("unclaimedFee", DoubleType(), True),
            StructField("options", ArrayType(StringType()), True)
        ]), True),
        StructField("rows", ArrayType(row_schema), True)
    ]))

    # Step 2: Parse JSON string column into array<struct>
    df_parsed = df.withColumn("buckets_struct", from_json(col("buckets"), bucket_schema))

    # Step 3: Explode buckets array
    df_buckets = df_parsed.withColumn("bucket", explode(col("buckets_struct")))
    df_buckets = (
        df_buckets.withColumn("bucket_bk", concat_ws("_", col("orderNumber"), col("status_value"), col("bucket.bucketNumber")))
            .withColumn("bucket_hashkey", md5(concat_ws("_", col("orderNumber"), col("status_value"), col("bucket.bucketNumber"))))
    )

    # Step 4: Select flattened fields
    df_bucket = df_buckets.select(
        col("bucket_bk").alias("bucket_bk"),
        col("bucket_hashkey").alias("bucket_hashkey"),
        col("orderNumber").alias("orderNumber"),
        col("status_value").alias("status_value"),
        col("bucket.bucketNumber").alias("BucketNumber"),
        col("bucket.returnCode").alias("ReturnCode"),

        col("bucket.deliveredFrom.name").alias("DeliveredFrom_name"),
        
        col("bucket.deliveryAddress.firstName").alias("deliveryAddress_FirstName"),
        col("bucket.deliveryAddress.lastName").alias("deliveryAddress_LastName"),
        col("bucket.deliveryAddress.address.street").alias("deliveryAddress_address_Street"),
        col("bucket.deliveryAddress.address.postalCode").alias("deliveryAddress_address_PostalCode"),
        col("bucket.deliveryAddress.address.city").alias("deliveryAddress_address_City"),
        col("bucket.deliveryAddress.address.countryCode").alias("deliveryAddress_address_CountryCode"),
        col("bucket.deliveryAddress.email").alias("deliveryAddress_Email"),
        col("bucket.deliveryAddress.mobilePhone").alias("deliveryAddress_MobilePhone"),

        col("bucket.pickupPoint.id").alias("pickupPoint_Id"),
        col("bucket.pickupPoint.name").alias("pickupPoint_Name"),
        col("bucket.pickupPoint.address.street").alias("pickupPoint_address_Street"),
        col("bucket.pickupPoint.address.postalCode").alias("pickupPoint_address_PostalCode"),
        col("bucket.pickupPoint.address.city").alias("pickupPoint_address_City"),
        col("bucket.pickupPoint.address.countryCode").alias("pickupPoint_address_CountryCode"),

        col("bucket.deliveryEstimate.date").alias("deliveryEstimate_Date"),
        col("bucket.deliveryEstimate.text").alias("deliveryEstimate_Text"),

        col("bucket.shipping.carrier.code").alias("shipping_carrier_Code"),
        col("bucket.shipping.carrier.name").alias("shipping_carrier_Name"),

        col("bucket.shipping.method").alias("shipping_Method"),
        col("bucket.shipping.title").alias("shipping_Title"),
        col("bucket.shipping.fee").alias("shipping_Fee"),
        col("bucket.shipping.returnFee").alias("shipping_ReturnFee"),
        col("bucket.shipping.unclaimedFee").alias("shipping_UnclaimedFee"),
        col("updated_datetime").alias("updated_datetime")
    )

    return df_bucket


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Transform Row

# CELL ********************

def transform_row(df):

    print(f"{datetime.now()} ℹ️ transform_row")

    # Step 1: Define the nested schema
    source_schema = StructType([
        StructField("name", StringType()),
        StructField("title", StringType()),
        StructField("isExternal", BooleanType())
    ])

    fraud_schema = StructType([
        StructField("value", StringType()),
        StructField("title", StringType())
    ])

    delivery_schema = StructType([
        StructField("value", StringType()),
        StructField("title", StringType())
    ])

    row_schema = StructType([
        StructField("rowNumber", IntegerType(), True),
        StructField("sku", StringType(), True),
        StructField("name", StringType(), True),
        StructField("color", StringType(), True),
        StructField("size", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("url", StringType(), True),
        StructField("imageName", StringType(), True),
        StructField("orderedQuantity", IntegerType(), True),
        StructField("deliveredQuantity", IntegerType(), True),
        StructField("cancelledQuantity", IntegerType(), True),
        StructField("notifiedReturnQuantity", IntegerType(), True),
        StructField("returnedQuantity", IntegerType(), True),
        StructField("singleBasePrice", DoubleType(), True),
        StructField("singleSalesPrice", DoubleType(), True),
        StructField("offers", ArrayType(StructType([
            StructField("campaignId", StringType(), True),
            StructField("discount", DoubleType(), True)
        ])), True),
        StructField("manualDiscount", DoubleType(), True),
        StructField("totalDiscount", DoubleType(), True),
        StructField("totalAmount", DoubleType(), True),
        StructField("vatRate", DoubleType(), True)
    ])

    bucket_schema = ArrayType(StructType([
        StructField("bucketNumber", IntegerType(), True),
        StructField("returnCode", StringType(), True),
        StructField("deliveredFrom", StructType([
            StructField("name", StringType(), True)
        ]), True),
        StructField("deliveryAddress", StructType([
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("address", StructType([
                StructField("street", StringType(), True),
                StructField("postalCode", StringType(), True),
                StructField("city", StringType(), True),
                StructField("countryCode", StringType(), True)
            ]), True),
            StructField("email", StringType(), True),
            StructField("mobilePhone", StringType(), True)
        ]), True),
        StructField("pickupPoint", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("address", StructType([
                StructField("street", StringType(), True),
                StructField("postalCode", StringType(), True),
                StructField("city", StringType(), True),
                StructField("countryCode", StringType(), True)
            ]), True)
        ]), True),
        StructField("deliveryEstimate", StructType([
            StructField("date", StringType(), True),
            StructField("text", StringType(), True)
        ]), True),
        StructField("shipping", StructType([
            StructField("carrier", StructType([
                StructField("code", StringType(), True),
                StructField("name", StringType(), True)
            ]), True),
            StructField("method", StringType(), True),
            StructField("title", StringType(), True),
            StructField("fee", DoubleType(), True),
            StructField("returnFee", DoubleType(), True),
            StructField("unclaimedFee", DoubleType(), True),
            StructField("options", ArrayType(StringType()), True)
        ]), True),
        StructField("rows", ArrayType(row_schema), True)
    ]))

    # Step 2: Parse JSON string column into array<struct>
    df_parsed = (
        df.withColumn("source", F.from_json("source", source_schema))
        .withColumn("fraudStatus", F.from_json("fraudStatus", fraud_schema))
        .withColumn("deliveryStatus", F.from_json("deliveryStatus", delivery_schema))    
        .withColumn("buckets_struct", from_json(col("buckets"), bucket_schema))
    )

    # Step 3: Explode buckets array
    df_buckets = df_parsed.withColumn("bucket", explode(col("buckets_struct")))
    df_buckets = (
        df_buckets.withColumn("bucket_bk", concat_ws("_", col("orderNumber"), col("status_value"), col("bucket.bucketNumber")))
            .withColumn("bucket_hashkey", md5(concat_ws("_", col("orderNumber"), col("status_value"), col("bucket.bucketNumber"))))
    )

    # Step 4: Explode rows array
    df_rows = df_buckets.withColumn("row", explode(col("bucket.rows")))
    df_rows = (
        df_rows.withColumn("row_bk", concat_ws("_", col("orderNumber"), col("status_value"), col("bucket.bucketNumber"), col("row.rowNumber")))
        .withColumn("row_hashkey", md5(concat_ws("_", col("orderNumber"), col("status_value"), col("bucket.bucketNumber"), col("row.rowNumber"))))
    )

    # Step 5: Select flattened fields
    df_row = df_rows.select(
        col("row_bk").alias("row_bk"),
        col("row_hashkey").alias("row_hashkey"),
        col("orderNumber").alias("orderNumber"),
        col("status_value").alias("status_value"),
        col("bucket.bucketNumber").alias("BucketNumber"),
        col("row.rowNumber").alias("RowNumber"),

        col("row.sku").alias("SKU"),
        col("row.name").alias("Name"),
        col("row.color").alias("Color"),
        col("row.size").alias("Size"),
        col("row.brand").alias("Brand"),

        col("row.url").alias("URL"),
        col("row.imageName").alias("ImageName"),
        col("row.orderedQuantity").alias("OrderedQuantity"),
        col("row.deliveredQuantity").alias("DeliveredQuantity"),
        col("row.cancelledQuantity").alias("CancelledQuantity"),
        col("row.notifiedReturnQuantity").alias("NotifiedReturnQuantity"),
        col("row.returnedQuantity").alias("ReturnedQuantity"),
        col("row.singleBasePrice").alias("SingleBasePrice"),
        col("row.singleSalesPrice").alias("SingleSalesPrice"),
        col("row.manualDiscount").alias("ManualDiscount"),
        col("row.totalDiscount").alias("TotalDiscount"),
        col("row.totalAmount").alias("TotalAmount"),
        col("row.vatRate").alias("VATRate"),
        col("updated_datetime").alias("updated_datetime"),

        # added 2025-09-30
        col("orderDate").alias("orderDate"),
        col("salesBrand").alias("salesBrand"),
        col("customerNumber").alias("customerNumber"),
        col("country").alias("country"),
        col("currency").alias("currency"),

        col("source.isExternal").alias("source_isExternal"),
        col("deliveryStatus.value").alias("deliveryStatus_value"),
        col("fraudStatus.value").alias("fraudStatus_value"),

        col("source.name").alias("source_name")
    )

    return df_row


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Transform OfferLine

# CELL ********************

def transform_offerline(df):

    print(f"{datetime.now()} ℹ️ transform_offerline")

    # Step 1: Define the nested schema
    row_schema = StructType([
        StructField("rowNumber", IntegerType(), True),
        StructField("sku", StringType(), True),
        StructField("name", StringType(), True),
        StructField("color", StringType(), True),
        StructField("size", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("url", StringType(), True),
        StructField("imageName", StringType(), True),
        StructField("orderedQuantity", IntegerType(), True),
        StructField("deliveredQuantity", IntegerType(), True),
        StructField("cancelledQuantity", IntegerType(), True),
        StructField("notifiedReturnQuantity", IntegerType(), True),
        StructField("returnedQuantity", IntegerType(), True),
        StructField("singleBasePrice", DoubleType(), True),
        StructField("singleSalesPrice", DoubleType(), True),
        StructField("offers", ArrayType(StructType([
            StructField("campaignId", StringType(), True),
            StructField("discount", DoubleType(), True)
        ])), True),
        StructField("manualDiscount", DoubleType(), True),
        StructField("totalDiscount", DoubleType(), True),
        StructField("totalAmount", DoubleType(), True),
        StructField("vatRate", DoubleType(), True)
    ])

    bucket_schema = ArrayType(StructType([
        StructField("bucketNumber", IntegerType(), True),
        StructField("returnCode", StringType(), True),
        StructField("deliveredFrom", StructType([
            StructField("name", StringType(), True)
        ]), True),
        StructField("deliveryAddress", StructType([
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("address", StructType([
                StructField("street", StringType(), True),
                StructField("postalCode", StringType(), True),
                StructField("city", StringType(), True),
                StructField("countryCode", StringType(), True)
            ]), True),
            StructField("email", StringType(), True),
            StructField("mobilePhone", StringType(), True)
        ]), True),
        StructField("pickupPoint", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("address", StructType([
                StructField("street", StringType(), True),
                StructField("postalCode", StringType(), True),
                StructField("city", StringType(), True),
                StructField("countryCode", StringType(), True)
            ]), True)
        ]), True),
        StructField("deliveryEstimate", StructType([
            StructField("date", StringType(), True),
            StructField("text", StringType(), True)
        ]), True),
        StructField("shipping", StructType([
            StructField("carrier", StructType([
                StructField("code", StringType(), True),
                StructField("name", StringType(), True)
            ]), True),
            StructField("method", StringType(), True),
            StructField("title", StringType(), True),
            StructField("fee", DoubleType(), True),
            StructField("returnFee", DoubleType(), True),
            StructField("unclaimedFee", DoubleType(), True),
            StructField("options", ArrayType(StringType()), True)
        ]), True),
        StructField("rows", ArrayType(row_schema), True)
    ]))

    # Step 2: Parse JSON string column into array<struct>
    df_parsed = df.withColumn("buckets_struct", from_json(col("buckets"), bucket_schema))

    # Step 3: Explode buckets array
    df_buckets = df_parsed.withColumn("bucket", explode(col("buckets_struct")))
    df_buckets = (
        df_buckets.withColumn("bucket_bk", concat_ws("_", col("orderNumber"), col("status_value"), col("bucket.bucketNumber")))
            .withColumn("bucket_hashkey", md5(concat_ws("_", col("orderNumber"), col("status_value"), col("bucket.bucketNumber"))))
    )

    # Step 4: Explode rows array
    df_rows = df_buckets.withColumn("row", explode(col("bucket.rows")))
    df_rows = (
        df_rows.withColumn("row_bk", concat_ws("_", col("orderNumber"), col("status_value"), col("bucket.bucketNumber"), col("row.rowNumber")))
        .withColumn("row_hashkey", md5(concat_ws("_", col("orderNumber"), col("status_value"), col("bucket.bucketNumber"), col("row.rowNumber"))))
    )

    # Step 5: Explode offers array (optional: if you want one row per offer)
    df_offers = df_rows.withColumn("offer", explode(col("row.offers")))
    df_offers = (
        df_offers.withColumn("offerline_bk", concat_ws("_", col("orderNumber"), col("status_value"), col("bucket.bucketNumber"), col("row.rowNumber"), col("offer.campaignId")))
        .withColumn("offerline_hashkey", md5(concat_ws("_", col("orderNumber"), col("status_value"), col("bucket.bucketNumber"), col("row.rowNumber"), col("offer.campaignId"))))
    )

    # Step 6: Select flattened fields
    df_offerline = df_offers.select(
        col("offerline_bk").alias("offerline_bk"),
        col("offerline_hashkey").alias("offerline_hashkey"),
        col("orderNumber").alias("orderNumber"),
        col("status_value").alias("status_value"),

        col("bucket.bucketNumber").alias("BucketNumber"),
        col("row.rowNumber").alias("RowNumber"),

        col("offer.campaignId").alias("CampaignId"),
        col("offer.discount").alias("Discount"),
        col("updated_datetime").alias("updated_datetime")
    )
    
    return df_offerline


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Transform Configuration

# CELL ********************

transforms = {
    "order": transform_order,
    "payment": transform_payment,
    "offerhead": transform_offerhead,
    "bucket": transform_bucket,
    "row": transform_row,
    "offerline": transform_offerline
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Misc Functions

# MARKDOWN ********************

# ### Monitor all / Stop all / Read incremental changes via CDF

# CELL ********************

# ---------------------------
# Monitor all
# Stop all
# Read incremental changes via CDF
#   - Change Data Feed (CDF)
# ---------------------------

def monitor_all_silver():
    if not active_queries:
        print("⚠️ No active streams in active_queries")
        return
    for name, q in active_queries.items():
        print(f"Name: {name}")
        print(f"ID: {q.id}")
        print(f"Active: {q.isActive}")
        print(f"Status: {q.status}")
        print()


def stop_all_silver():
    for name, q in active_queries.items():
        if q.isActive:
            print(f"Stopping stream: {name} ({q.id})")
            q.stop()
    active_queries.clear()
    print("✅ All Silver streams stopped")


def read_cdf_incremental(starting_version=0):
    for name, cfg in silver_tables.items():
        table_name = cfg["table"]
        if not spark.catalog.tableExists(table_name):
            print(f"⚠️ Table {table_name} does not exist")
            continue

        df_changes = (spark.read.format("delta")
                             .option("readChangeFeed", "true")
                             .option("startingVersion", starting_version)
                             .table(table_name))

        if df_changes.rdd.isEmpty():
            print(f"⚹ No changes in {name}")
        else:
            print(f"🔹 Changes in {name}:")
            df_changes.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Reset tables etc.

# CELL ********************

# ---------------------------
# Reset function for one Silver table
# ---------------------------

def reset_table(name):
    """
    Reset a Silver table in Fabric:
    - Stop query if active
    - Drop Silver Delta table
    - Delete checkpoint folder
    """
    global active_queries
    
    # Stop query if active
    if name in active_queries and active_queries[name].isActive:
        print(f"⏹ Stopping active stream for {name}")
        active_queries[name].stop()
        del active_queries[name]
    
    # Drop Silver table
    table_name = silver_tables[name]["table"]
    if spark.catalog.tableExists(table_name):
        print(f"🗑 Dropping Silver table {table_name}")
        spark.sql(f"DROP TABLE {table_name}")
        
    '''    
        # Delete checkpoint
        checkpoint_path = f"{checkpoint_base}{name}"
        if mssparkutils.fs.exists(checkpoint_path):
            mssparkutils.fs.rm(checkpoint_path, recurse=True)
            print(f"🗑 Deleted checkpoint: {checkpoint_path}")
        else:
            print(f"⚠️ No checkpoint found at {checkpoint_path}")
    '''

    # Delete checkpoint multi_silver
    checkpoint_path = f"{checkpoint_base}{checkpoint_name}"
    if mssparkutils.fs.exists(checkpoint_path):
        mssparkutils.fs.rm(checkpoint_path, recurse=True)
        print(f"🗑 Deleted checkpoint: {checkpoint_path}")    

    
    print(f"✅ Reset complete for {name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------
# Reset all Silver table
# ---------------------------
def reset_all_tables():
    """
    Completely reset ALL Silver tables:
    - Stop all active queries
    - Drop each Delta table
    - Delete all checkpoint folders
    """
    for name in silver_tables.keys():
        reset_table(name)
    print("✅ Reset all done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Lock Helper

# CELL ********************

# ---------------------------
# Lock helpers
# ---------------------------

def ensure_lock_table():
    if not spark.catalog.tableExists(lock_table):
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {lock_table} (
              lock_name STRING,
              owner STRING,
              acquired_at TIMESTAMP,
              expiry_ts TIMESTAMP
            )
            USING DELTA""")
        print(f"{datetime.now()} Created lock table {lock_table}")


def acquire_lock(lock_name: str, ttl_minutes: int = 30) -> str | None:
    
    lock_owner = str(uuid.uuid4())

    # Only update if the lock has expired.
    '''
    Avoid leaving stale locks
        - Suppose a notebook crashed and never called release_lock.
        - The row still exists in the table, with a past expiry_ts.
        - Without this update, nobody could ever acquire that lock again.
    '''
    sql = f"""
    MERGE INTO {lock_table} AS t
    USING (
        SELECT
            '{lock_name}' AS lock_name,
            '{lock_owner}' AS owner,
            current_timestamp() AS acquired_at,
            current_timestamp() + INTERVAL {ttl_minutes} MINUTES AS expiry_ts
    ) AS s
    ON t.lock_name = s.lock_name

    WHEN MATCHED AND t.expiry_ts <= current_timestamp() 
      THEN UPDATE SET
          t.owner = s.owner,
          t.acquired_at = s.acquired_at,
          t.expiry_ts = s.expiry_ts

    WHEN NOT MATCHED
      THEN INSERT (lock_name, owner, acquired_at, expiry_ts)
           VALUES (s.lock_name, s.owner, s.acquired_at, s.expiry_ts)
    """
    spark.sql(sql)

    # Verify if we now own the lock
    result = spark.sql(f"""
        SELECT owner
        FROM {lock_table}
        WHERE lock_name = '{lock_name}'
    """).collect()

    if result and result[0]["owner"] == lock_owner:
        print(f"{datetime.now()} 🔒 Lock acquired: {lock_name} by owner {lock_owner}")
        return lock_owner
    else:
        print(f"{datetime.now()} ⚠️ Failed to acquire lock: {lock_name}, held by {result[0]['owner'] if result else 'unknown'}")
        return None


def release_lock(lock_name: str, lock_owner: str) -> bool:

    sql = f"""
    DELETE FROM {lock_table}
    WHERE lock_name = '{lock_name}' AND owner = '{lock_owner}'
    """
    result = spark.sql(sql)

    if result.count() == 0:
        print(f"{datetime.now()} ⚠️ Release failed: {lock_name} not owned by {lock_owner} or lock missing")
        return False
    else:
        print(f"{datetime.now()} 🔓 Lock released: {lock_name} by owner {lock_owner}")
        return True


def cleanup_expired_locks():

    now = datetime.utcnow()
    print(f"{datetime.now()} 🧹 Cleaning up locks older than {now} (UTC)...")
    spark.sql(f"""
        DELETE FROM {lock_table}
        WHERE expiry_ts <= current_timestamp()
    """)
    remaining = spark.sql(f"SELECT * FROM {lock_table}").count()
    print(f"{datetime.now()} ✅ Cleanup complete. Remaining active locks: {remaining}")
    

def delete_all_locks():

    spark.sql(f"""
        DELETE FROM {lock_table}
    """)
    print(f"{datetime.now()} ✅ Remove all locks complete.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Upsert all silver

# CELL ********************

# ---------------------------
# Generic merge function
# ---------------------------

def merge_to_silver(df, table_name, keys):
    if df.rdd.isEmpty():
        print(f"{datetime.now()} ⚠️ No rows for {table_name} in this batch")
        return

    if not spark.catalog.tableExists(table_name):
        print(f"{datetime.now()} ℹ️ Creating Silver table {table_name} with CDF enabled")
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        print(f"{datetime.now()} ℹ️ Creating Silver table {table_name} with CDF DONE")
        return

    delta_table = DeltaTable.forName(spark, table_name)
    merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in keys])

    print(f"{datetime.now()} ✅ Silver table {table_name} upsert starting")

    (delta_table.alias("t")
        .merge(df.alias("s"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

    print(f"{datetime.now()} ✅ Silver table {table_name} upsert complete")
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------
# Upsert function for Silver
# ---------------------------

def upsert_all_silver(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    print(f"{datetime.now()} ⚡ Processing batch {batch_id}, rows: {batch_df.count()}")

    # ✅ Apply preprocessing here
    df_preprocessed = preprocess_bronze(batch_df)

    # Keep the latest per orderNumber and status_value
    windowSpec = Window.partitionBy("orderNumber", "status_value").orderBy(F.desc("orderDate"))
    df_dedup = df_preprocessed.withColumn("row_num", F.row_number().over(windowSpec)) \
                .filter(F.col("row_num") == 1) \
                .drop("row_num")    

    for name, cfg in silver_tables.items():
        print(f"{datetime.now()} ▶ Processing {name} -> {cfg['table']}")
        df_silver = transforms[name](df_dedup)
        merge_to_silver(df_silver, cfg["table"], cfg["keys"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Start all silver

# CELL ********************

# ---------------------------
# Controller: Active queries
# ---------------------------
active_queries = {}

def start_all_silver():
    global active_queries
    ensure_lock_table()

    # Try to get a lock...
    lock_owner = acquire_lock(lock_name, lock_ttl)
    if not lock_owner:
        mssparkutils.notebook.exit(0)

    try:

        while True:

            '''        
                if not acquire_lock(lock_name, lock_owner, lock_ttl):
                    print("⚠️ Another run already holds the lock. Exiting.")
                    mssparkutils.notebook.exit("lock_not_acquired")
            '''

            print(f"{datetime.now()} ✅ Batch starting")
            query = (
                spark.readStream.table(eventstream_table)
                    .writeStream
                    .foreachBatch(upsert_all_silver)
                    .option("checkpointLocation", f"{checkpoint_base}{checkpoint_name}")
                    .trigger(once=True)   # <--- important
                    .start()
            )
            active_queries[f"{checkpoint_name}"] = query
            query.awaitTermination()
            print(f"{datetime.now()} ✅ Batch completed")

            print(f"{datetime.now()} ✅ Sleeping for 5 minutes")
            
            time.sleep(5*60)

    finally:
        release_lock(lock_name, lock_owner)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Misc

# CELL ********************

'''
# Try to get a lock...
lock_owner = acquire_lock(lock_name, lock_ttl)
'''
'''
# Release lock
lockresult = release_lock(lock_name, lock_owner)
'''

print()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Start Execution

# CELL ********************

# ---------------------------
# Run mode
# ---------------------------
print(f"{datetime.now()} ✅ Execution start")

# Safety check
if spark.streams.active:
    print("ℹ️ Another batch is still running. Exiting.")
    mssparkutils.notebook.exit(0)

'''
if mode == "start":
    start_all_silver()
elif mode == "monitor":
    monitor_all_silver()j
elif mode == "stop":
    stop_all_silver()
elif mode == "cdf_read":
    read_cdf_incremental(starting_version=0)
else:
    raise ValueError(f"Unknown mode: {mode}")
'''


start_all_silver()

# monitor_all_silver()
# stop_all_silver()

# read_cdf_incremental(starting_version=0)
# reset_table("bucket")
# reset_all_tables()

# cleanup_expired_locks()
# delete_all_locks()

print("")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
