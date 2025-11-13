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
# META     }
# META   }
# META }

# CELL ********************

import concurrent.futures
import threading
import time
import traceback
from tqdm.notebook import tqdm # Use tqdm.notebook for better notebook integration

# --- Configuration ---
DATABASE_NAME = 'stream'      # e.g., "my_lakehouse_db" or None for default
TABLE_PREFIX = ""         # e.g., "fact_" or "" for all tables
MAX_PARALLEL_WORKERS = 5  # Adjust parallelism
VACUUM_RETAIN_HOURS = 168 # Explicit retention (e.g., 168 for 7 days). Use None for default VACUUM. Avoid 0.

# --- Helper Function to Run SQL Commands ---
# (Handles SQL execution, view checks, basic errors - slightly updated for clarity)
def _run_spark_sql(sql, table_name, operation, thread_id):
    """Runs Spark SQL, handles view errors, returns status dict."""
    try:
        # print(f"[Thread-{thread_id}] Running {operation} on {table_name}: {sql}") # Log the command being run
        spark.sql(sql)
        return {"success": True, "error": None, "is_view": False}
    except Exception as e:
        error_msg = str(e).splitlines()[0] # Get first line of error
        is_view = "is not supported for view" in error_msg or "is a view" in error_msg
        
        if is_view:
            # Log view detection specifically
            print(f"[Thread-{thread_id}] INFO: Detected {table_name} as view during {operation}. Skipping.")
            return {"success": False, "error": "Detected as view", "is_view": True}
        else:
            # Specific check for VACUUM retention error
            if operation == "VACUUM" and "retentionDurationCheck" in error_msg:
                 error_msg = f"VACUUM failed: Retention check. Use VACUUM_RETAIN_HOURS setting."
            
            # Log actual errors
            print(f"[Thread-{thread_id}] WARN Error during {operation} on {table_name}: {error_msg}")
            # Consider printing full traceback for unexpected errors if needed during debugging
            # traceback.print_exc() 
            return {"success": False, "error": error_msg, "is_view": False}

# --- Worker Function
def run_maintenance_on_table(full_table_name):
    """Runs OPTIMIZE (with VORDER for 'gold' tables) and VACUUM sequentially."""
    thread_id = threading.get_ident()
    start_time = time.time()
    result = {"table_name": full_table_name, "is_view": False}

    # Extract base table name (handle potential database prefix)
    table_parts = full_table_name.split('.')
    base_table_name = table_parts[-1]

    optimize_sql = f"OPTIMIZE {full_table_name} VORDER"

    opt_res = _run_spark_sql(optimize_sql, full_table_name, "OPTIMIZE", thread_id)
    result.update({"optimized": opt_res["success"], "optimize_error": opt_res["error"], "is_view": opt_res["is_view"]})

    # 2. Vacuum (only if OPTIMIZE didn't detect it as a view)
    if not result["is_view"]:
        if VACUUM_RETAIN_HOURS is not None:
            vac_sql = f"VACUUM {full_table_name} RETAIN {VACUUM_RETAIN_HOURS} HOURS"
        else:
            vac_sql = f"VACUUM {full_table_name}" # Default retention (may fail check)
        
        vac_res = _run_spark_sql(vac_sql, full_table_name, "VACUUM", thread_id)
        result.update({"vacuumed": vac_res["success"], "vacuum_error": vac_res["error"]})
        # No need to check is_view again, OPTIMIZE should have caught it. 
        # If VACUUM *could* detect a view missed by OPTIMIZE, you might add:
        # if vac_res["is_view"]: result["is_view"] = True # Redundant if OPTIMIZE check is reliable
    else:
        result.update({"vacuumed": False, "vacuum_error": "Skipped (View detected by OPTIMIZE)"}) # Mark skipped

    result["duration"] = time.time() - start_time
    status = ("View" if result["is_view"] else
              f"Opt:{'OK' if result['optimized'] else 'Fail'}|Vac:{'OK' if result.get('vacuumed') else 'Fail'}")
    print(f"[Thread-{thread_id}] Finished {full_table_name} ({status}) in {result['duration']:.2f}s") # Optional: uncomment if detailed logs per table needed
    return result

# --- Main Execution ---
start_time_main = time.time()
results_list = []
print(f"Starting maintenance: DB='{DATABASE_NAME or 'Default'}', Prefix='{TABLE_PREFIX or 'None'}', Parallelism={MAX_PARALLEL_WORKERS}")
if VACUUM_RETAIN_HOURS is not None: print(f"VACUUM using RETAIN {VACUUM_RETAIN_HOURS} HOURS.")
if VACUUM_RETAIN_HOURS == 0: print("WARNING: VACUUM RETAIN 0 HOURS is risky.")

try:
    # --- List and Filter Tables (Concise) ---
    target_db = DATABASE_NAME if DATABASE_NAME else spark.catalog.currentDatabase()
    print(f"Listing & filtering tables/views in '{target_db}'...")
    all_spark_tables = spark.catalog.listTables(DATABASE_NAME)

    prefix_filter = lambda name: name.startswith(TABLE_PREFIX) if TABLE_PREFIX else True
    # Ensure fully qualified name handling, assuming default DB if not specified in table object
    def get_fqn(t):
        db = t.database if t.database else target_db
        return f"{db}.{t.name}"
        
    get_table_part = lambda fqn: fqn.split('.')[-1]

    tables_to_process = [
        fqn for t in all_spark_tables if not t.isTemporary
        and prefix_filter(get_table_part(fqn := get_fqn(t))) # Use assignment expression
    ]

    if not tables_to_process:
        print("No tables/views match criteria.")
    else:
        print(f"Found {len(tables_to_process)} items to process.")
        print(f"--- Starting Parallel Maintenance ---")
        # --- Parallel Execution with TQDM Progress Bar ---
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as executor:
            futures = [executor.submit(run_maintenance_on_table, name) for name in tables_to_process]

            # Wrap as_completed with tqdm for progress tracking
            for future in tqdm(concurrent.futures.as_completed(futures),
                               total=len(futures),              # Total number of tasks
                               desc="Table Maintenance",        # Progress bar description
                               unit="table"):                   # Unit shown in the bar
                try:
                    results_list.append(future.result())
                except Exception as e:
                    # Handle errors getting result from future
                    print(f"\nERROR: Task execution failed unexpectedly: {e}") # Newline to avoid tqdm overlap
                    # Log more details if possible, e.g., which table might have failed if identifiable
                    # traceback.print_exc() # Uncomment for full stack trace during debugging
                    results_list.append({"table_name": "Unknown (Task Failed)", 
                                         "optimized": False, "vacuumed": False, 
                                         "optimize_error": str(e), "vacuum_error": str(e)})

        # --- Concise Summary ---
        print("\n--- Maintenance Summary ---") # Add newline after tqdm
        total = len(results_list)
        views = sum(r.get("is_view", False) for r in results_list)
        opt_ok = sum(r.get("optimized", False) for r in results_list)
        vac_ok = sum(r.get("vacuumed", False) for r in results_list)
        # Filter out view errors from genuine operation errors for summary
        opt_err = sum(1 for r in results_list if not r.get("is_view") and not r.get("optimized") and r.get("optimize_error"))
        vac_err = sum(1 for r in results_list if not r.get("is_view") and r.get("vacuum_error") and not r.get("vacuumed", False) and "Skipped" not in r.get("vacuum_error","")) # Count only actual VACUUM errors, not skips

        print(f"Items Processed: {total} | Views Skipped: {views}")
        print(f"Optimize OK: {opt_ok} | Vacuum OK: {vac_ok}")
        print(f"Optimize Errors (non-views): {opt_err} | Vacuum Errors (non-views): {vac_err}")

        # Print specific errors if any occurred on non-views
        errors = [r for r in results_list if not r.get("is_view") and (r.get("optimize_error") or (r.get("vacuum_error") and "Skipped" not in r.get("vacuum_error","")))]
        if errors:
            print("\n--- Error Details (Non-Views) ---")
            for r in errors:
                if r.get("optimize_error") and not r.get("optimized"): 
                   print(f"  OPT {r['table_name']}: {r['optimize_error']}")
                if r.get("vacuum_error") and not r.get("vacuumed", False) and "Skipped" not in r.get("vacuum_error",""): 
                   print(f"  VAC {r['table_name']}: {r['vacuum_error']}")

except Exception as e:
    print(f"\nCRITICAL SCRIPT ERROR: An unexpected error occurred in the main script execution.")
    print(f"Error: {e}")
    traceback.print_exc()

finally:
    duration = time.time() - start_time_main
    print(f"\n--- Script Finished ({duration:.2f}s / {duration / 60:.2f}min) ---")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(spark.sql("show tables in stream"))
# abfss://fb736e26-5db5-424b-8125-6f2ab29a83f0@onelake.dfs.fabric.microsoft.com/408d8412-c973-46aa-8c0c-52dbe1c38ada/Tables/stream/orderupdated_rowstream

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
