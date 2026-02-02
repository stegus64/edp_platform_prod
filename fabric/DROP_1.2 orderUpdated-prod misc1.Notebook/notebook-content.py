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

# ================================
# 
# ================================

# 

# 
# 
# 
# 
# 
# 

'''
spark.sql("SELECT count(1) FROM lh_edp_prod.stream.orderupdated_raw LIMIT 10").show()

spark.sql("SELECT count(1) FROM lh_edp_prod.dbo.orderupdated_order LIMIT 10").show()
spark.sql("SELECT count(1) FROM lh_edp_prod.dbo.orderupdated_payment LIMIT 10").show()
spark.sql("SELECT count(1) FROM lh_edp_prod.dbo.orderupdated_offerhead LIMIT 10").show()
spark.sql("SELECT count(1) FROM lh_edp_prod.dbo.orderupdated_bucket LIMIT 10").show()
spark.sql("SELECT count(1) FROM lh_edp_prod.dbo.orderupdated_row LIMIT 10").show()
spark.sql("SELECT count(1) FROM lh_edp_prod.dbo.orderupdated_offerline LIMIT 10").show()

spark.sql("SELECT *  FROM lh_edp_prod.stream.orderupdated_raw order by EventProcessedUtcTime desc LIMIT 100").show()
spark.sql("SELECT *  FROM lh_edp_prod.stream.orderupdated_cleaned order by EventProcessedUtcTime desc LIMIT 100").show()

df = spark.sql("SELECT * FROM lh_edp_prod.stream.orderupdated_raw LIMIT 1000")
display(df)
df = spark.sql("SELECT * FROM lh_edp_prod.stream.orderupdated_cleaned LIMIT 1000")
display(df)

# spark.sql("DELETE FROM lh_edp_test.stream.orderupdated_locks")

df = spark.sql("SELECT * FROM lh_edp_prod.stream.orderupdated_locks LIMIT 1000")
display(df)

'''

print("")   # Do not display commented part...

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.sql("SELECT * FROM lh_edp_prod.stream.orderupdated_locks")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql import functions as F
from datetime import datetime
import pytz

testtime = "2025-09-18T06:05:10Z"
testtime2 = getdatetime()
print(testtime2)

utc_time = datetime.strptime("2025-09-18T06:05:10", "%Y-%m-%dT%H:%M:%S")
#utc_time = datetime.strptime(getdatetime(), "%Y-%m-%dT%H:%M:%S")
sweden_tz = pytz.timezone("Europe/Stockholm")
local_time = utc_time.replace(tzinfo=pytz.utc).astimezone(sweden_tz)
print(local_time)


from pyspark.sql.functions import col, from_utc_timestamp
time_se = from_utc_timestamp(testtime, "Europe/Stockholm")
print(time_se)


time_se =  F.from_utc_timestamp(F.to_utc_timestamp(testtime, "UTC"), "Europe/Stockholm")
print(time_se)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import pyspark.sql.functions as F
from pyspark.sql.functions import col, from_utc_timestamp

df = spark.sql("SELECT * FROM lh_edp_prod.stream.orderupdated_raw LIMIT 1000")
df = df.select("orderNumber", "orderDate", "salesBrand")

df = df.withColumn("od_org", F.to_timestamp("orderDate"))

# Ok
df = df.withColumn("orderDatexx",from_utc_timestamp(col("orderDate"), "Europe/Stockholm"))
#df = df.withColumn("orderDate", F.to_timestamp("orderDate"))
#df = df.withColumn("od_ts", F.to_timestamp("orderDate"))


display(df)









# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
