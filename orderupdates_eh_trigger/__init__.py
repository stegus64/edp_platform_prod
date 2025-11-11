import logging
import os
import tempfile
from typing import Iterable, List
import concurrent.futures
import azure.functions as func
import duckdb
from datetime import datetime

from shared import write_with_retry, upload_to_inbound_storage, schema

app = func.Blueprint()

# --- Config -----------------------------------------------------------------

EVENT_HUB_NAME = "evh-orderupdated-prod"
EVENT_HUB_CONN = "eventhub_connectionstring"
EVENT_HUB_CONSUMER = os.getenv('eventhub_consumer_group_4', 'func-edp-prod-local')


# --- Helpers ----------------------------------------------------------------

skip_general_deadletter = False

def _write_ndjson_temp_with_meta(events: Iterable[func.EventHubEvent], path: str):
    """
    Write newline-delimited JSON (event body + selected EventHubEvent metadata) to the given path.
    """
    with open(path, "wb") as f:
        for e in events:
            body_bytes = e.get_body()
            body_str = body_bytes.decode("utf-8").rstrip()

            meta_json = (
                f',"EventEnqueuedUtcTime":"{e.enqueued_time.isoformat().replace("+00:00", "Z")}"'
                f',"SequenceNumber":{e.sequence_number}'
                f',"Offset":"{e.offset}"'
                f',"PartitionKey":"{(e.partition_key or "").replace("\"", "\\\"")}"'
            )

            if body_str.endswith("}"):
                body_str = body_str[:-1] + meta_json + "}"
            else:
                body_str = f'{{"body":{body_str if body_str else "null"}{meta_json}}}'

            f.write(body_str.encode("utf-8"))
            f.write(b"\n")

# ---------- DuckDB helper (per-thread connection) ----------
def _query_df(query: str):
    # Independent connection per task/thread
    # ':memory:' is fine because we're reading from a file path, not a DB file.
    with duckdb.connect(database=':memory:', read_only=False) as con:
        # (Optional) tweak parallelism inside DuckDB if desired:
        # con.execute("PRAGMA threads=4")
        return con.execute(query).arrow()

def _transform_buckets(ndjson_path: str):
    """
    Run DuckDB SQL to explode nested arrays and project fields into a tabular dataframe.
    Returns a pandas.DataFrame (DuckDB .df()) suitable for write_delta.
    """

    query = f"""
    WITH src AS (
      SELECT *
      FROM read_json_auto('{ndjson_path}')
    ),
    bucket_explode AS (
      SELECT
        -- business keys
        concat(e.orderNumber, '_', b.bucket.bucketNumber)                                   AS bucket_bk,
        concat(e.orderNumber, '_', b.bucket.bucketNumber, '_', r.row.rowNumber)             AS row_bk,

        -- from event + nested arrays
        e.orderNumber,
        b.bucket.bucketNumber                                                                AS BucketNumber,
        r.row.rowNumber                                                                       AS RowNumber,
        r.row.sku                                                                             AS SKU,
        r.row.name                                                                            AS Name,
        r.row.color                                                                           AS Color,
        r.row.size                                                                            AS Size,
        r.row.brand                                                                           AS Brand,
        r.row.url                                                                             AS URL,
        r.row.imageName                                                                       AS ImageName,
        r.row.orderedQuantity                                                                 AS OrderedQuantity,
        r.row.deliveredQuantity                                                               AS DeliveredQuantity,
        r.row.cancelledQuantity                                                               AS CancelledQuantity,
        r.row.notifiedReturnQuantity                                                          AS NotifiedReturnQuantity,
        r.row.returnedQuantity                                                                AS ReturnedQuantity,
        r.row.singleBasePrice                                                                 AS SingleBasePrice,
        r.row.singleSalesPrice                                                                AS SingleSalesPrice,
        r.row.manualDiscount                                                                  AS ManualDiscount,
        r.row.totalDiscount                                                                   AS TotalDiscount,
        r.row.totalAmount                                                                     AS TotalAmount,
        r.row.vatRate                                                                         AS VATRate,

        -- additional fields from main event
        CAST(e.orderDate AS TIMESTAMPTZ)                                                      AS orderDate,
        e.salesBrand,
        e.customerNumber,
        e.country,
        e.currency,
        e.status.value                                                                        AS status_value,
        e.source.isExternal                                                                   AS source_isExternal,
        e.source.name                                                                         AS source_name,
        e.deliveryStatus.value                                                                AS deliveryStatus_value,
        e.fraudStatus.value                                                                   AS fraudStatus_value,

        -- ingestion metadata
        CAST(e.EventEnqueuedUtcTime || ' UTC' AS TIMESTAMPTZ)                                 AS source_updated_datetime,
        CURRENT_TIMESTAMP                                                                      AS updated_datetime
      FROM src AS e
      -- Inner expansion (drop rows when arrays are empty/null). Use LEFT JOIN ... ON TRUE to preserve.
      CROSS JOIN UNNEST(e.buckets)     AS b(bucket)
      CROSS JOIN UNNEST(b.bucket.rows) AS r(row)
    )
    SELECT *
    FROM bucket_explode;
    """
    
    result = _query_df(query)
    
    return result


def _transform_orders(ndjson_path: str):
    """
    Re-creates the 'order_explode' streaming query against the same JSON source.
    """

    query = f"""
    WITH order_explode AS (
      SELECT
        -- Business key
        e.orderNumber                                   AS order_bk,

        -- Scalars from event
        e.orderNumber,
        e.status.value                                  AS status_value,
        e.orderDate                                     AS orderDate,
        e.salesBrand,
        e.country,
        e.currency,

        -- Source fields
        e.source.name                                   AS source_name,
        e.source.title                                  AS source_title,
        e.source.isExternal                             AS source_isExternal,

        -- Misc
        e.clientIP                                      AS clientIP,
        e.status.title                                  AS status_title,

        -- Fraud / delivery
        e.fraudStatus.value                             AS fraudStatus_value,
        e.fraudStatus.title                             AS fraudStatus_title,
        e.deliveryStatus.value                          AS deliveryStatus_value,
        e.deliveryStatus.title                          AS deliveryStatus_title,

        -- Customer
        e.customerNumber                                AS customerNumber,

        -- Invoice address (nested)
        e.invoiceAddress.firstName                      AS invoiceAddress_firstName,
        e.invoiceAddress.lastName                       AS invoiceAddress_lastName,
        e.invoiceAddress.email                          AS invoiceAddress_email,
        e.invoiceAddress.mobilePhone                    AS invoiceAddress_mobilePhone,
        e.invoiceAddress.address.street                 AS invoiceAddress_address_street,
        e.invoiceAddress.address.postalCode             AS invoiceAddress_address_postalCode,
        e.invoiceAddress.address.city                   AS invoiceAddress_address_city,
        e.invoiceAddress.address.countryCode            AS invoiceAddress_address_countryCode,

        -- Totals (nested)
        e.total.handlingFee                             AS total_handlingFee,
        e.total.shippingFee                             AS total_shippingFee,
        e.total.salesAmount                             AS total_salesAmount,
        e.total.discount                                AS total_discount,
        e.total.totalAmount                             AS total_totalAmount,
        e.total.creditedAmount                          AS total_creditedAmount,

        -- Ingestion/processing times
        CAST(e.EventEnqueuedUtcTime || ' UTC' AS TIMESTAMPTZ) AS EventEnqueuedUtcTime,
        CURRENT_TIMESTAMP                               AS updated_datetime
      FROM read_json_auto('{ndjson_path}') AS e
    )
    SELECT *
    FROM order_explode;
    """    
    result = _query_df(query)
    
    return result


def _transform_payments(ndjson_path: str):

    query = f"""
    WITH payment_explode AS (
      SELECT
        e.orderNumber                                     AS order_bk,
        
        concat(
            e.orderNumber::VARCHAR, '_',
            p.payment.title::VARCHAR, '_',
            p.payment.psp::VARCHAR, '_',
            p.payment.method::VARCHAR, '_',
            p.payment.pspReference::VARCHAR
        ) AS payment_bk,

        e.orderNumber,
        e.status.value                                    AS status_value,

        p.payment.title                                   AS title,
        p.payment.psp                                     AS psp,
        p.payment.method                                  AS method,
        p.payment.alternative                             AS alternative,
        p.payment.amount                                  AS amount,
        p.payment.pspReference                            AS pspReference,

        CAST(e.EventEnqueuedUtcTime || ' UTC' AS TIMESTAMPTZ) AS EventEnqueuedUtcTime,
        CURRENT_TIMESTAMP                                 AS updated_datetime
      FROM read_json_auto('{ndjson_path}') AS e
      CROSS JOIN UNNEST(e.payments) AS p(payment)
    )
    SELECT * FROM payment_explode;
    """
    
    result = _query_df(query)
    
    return result

def _transform_raw(ndjson_path: str):
    query = f"""
    SELECT *
    FROM read_json('{ndjson_path}',
        columns = {schema.orderupdate_schema}
    );
    """
    result = _query_df(query)
    return result


def store_deadletter(ndjson_path: str, prefix: str) -> None:
    """Upload a deadletter file with stage name, UTC timestamp, and optional sequence range."""

    # ts = datetime.now().strftime("%Y-%m-%dT%H-%M-%SZ")
  
    deadletter_path = f"Files/deadletters/{EVENT_HUB_CONSUMER}/{prefix}.json"

    with open(ndjson_path, "rb") as f:
        upload_to_inbound_storage(deadletter_path, f)

    logging.info("Stored deadletter: %s", deadletter_path)

def deadletter_on_exception(prefix: str, ndjson_path: str):
    """Decorator: on exception, store deadletter for this stage and re-raise."""
    def _wrap(fn):
        def _inner():
            try:
                return fn()
            except Exception as e:
                store_deadletter(ndjson_path, prefix)
                global skip_general_deadletter
                skip_general_deadletter = True
                logging.error("Stage '%s' failed: %s", prefix, e)
                raise
        return _inner
    return _wrap

# --- Trigger ----------------------------------------------------------------

@app.function_name(name="orderupdates_eh_trigger")
@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name=EVENT_HUB_NAME,
    connection=EVENT_HUB_CONN,
    consumer_group=EVENT_HUB_CONSUMER,
    cardinality=func.Cardinality.MANY,
)
@app.retry(strategy="exponential_backoff", 
           max_retry_count="-1",
           minimum_interval="00:00:01",
           maximum_interval="01:00:00")
def eventhub_trigger(event: List[func.EventHubEvent]) -> None:
    """
    Azure Functions Event Hub trigger: processes a batch of order update events.
    - Writes NDJSON with appended metadata to a temp file
    - Transforms with DuckDB (exploding nested arrays)
    - Persists to Delta via shared.write_delta
    """
    if not event:
        logging.info("EventHub trigger invoked with an empty batch; nothing to do.")
        return

    logging.info("Received batch of %d event(s).", len(event))
    
    seq_start = event[0].sequence_number
    seq_end = event[-1].sequence_number

    with tempfile.TemporaryDirectory() as tmpdir:
        ndjson_path = os.path.join(tmpdir, "events.jsonl")

        # Write NDJSON file with metadata
        _write_ndjson_temp_with_meta(event, path=ndjson_path)
        try:
            
            @deadletter_on_exception(f"buckets_{seq_start}-{seq_end}", ndjson_path)
            def process_buckets():
                bucket_df = _transform_buckets(ndjson_path)
                logging.info("Bucket explode produced %d row(s).", len(bucket_df))
                write_with_retry(bucket_df, table="orderupdates_rowstream", source="Tables/stream")
                return "buckets done"
            
            @deadletter_on_exception(f"orders_{seq_start}-{seq_end}", ndjson_path)
            def process_orders():
                orders_df = _transform_orders(ndjson_path)
                logging.info("Order stream produced %d row(s).", len(orders_df))
                write_with_retry(orders_df, table="orderupdates_orderstream", source="Tables/stream")
                return "orders done"
            
            @deadletter_on_exception(f"payments_{seq_start}-{seq_end}", ndjson_path)
            def process_payments():
                payments_df = _transform_payments(ndjson_path)
                logging.info("Payment stream produced %d row(s).", len(payments_df))
                write_with_retry(payments_df, table="orderupdates_paymentstream", source="Tables/stream")
                return "payments done"
            
            @deadletter_on_exception(f"raw_{seq_start}-{seq_end}", ndjson_path)
            def process_raw():
                raw_df = _transform_raw(ndjson_path)
                #logging.info("Raw stream produced %d row(s).", len(raw_df))
                write_with_retry(raw_df, table="orderupdates_rawstream", source="Tables/stream")
                return "raw done"
            
            # Run in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                futures = [
                    executor.submit(process_buckets),
                    executor.submit(process_orders),
                    executor.submit(process_payments),
                    executor.submit(process_raw),
                ]
                for future in concurrent.futures.as_completed(futures):
                    logging.info(future.result())

        except Exception as e:
            if not skip_general_deadletter:
                store_deadletter(ndjson_path, f"general_{seq_start}-{seq_end}")

            logging.error("Error processing events: %s", str(e))
            raise

        logging.info("All streams written successfully.")
