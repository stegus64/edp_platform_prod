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

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_edp_prod.stream.orderupdates_orderstream LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("describe history lh_edp_prod.stream.orderupdates_orderstream")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

props = spark.sql("SHOW TBLPROPERTIES lh_edp_prod.stream.orderupdates_orderstream")
display(props.filter("key like 'delta.%'"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
