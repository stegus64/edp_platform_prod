import polars as pl

orderupdate_schema = {
    "partitionKey": pl.String,
    "id": pl.String,
    "erpId": pl.String,
    "operationalEntityId": pl.String,
    "name": pl.String,
    "culture": pl.String,
    "defaultSite": pl.String,
    "erpIdentifier": pl.String,
    
    # Warehouse List Array
    "warehouseList": pl.List(pl.Struct({
        "warehouseCode": pl.String,
        "warehouseName": pl.String
    })),
    
    "division": pl.String,
    "selectedGacDivisions": pl.List(pl.String),
    "selectedGacCodes": pl.Struct({
        "key": pl.String,
        "value": pl.List(pl.String)
    }),  # Note: Polars doesn't have MapType, see note below
    "status": pl.String,
    "modified": pl.String,
    
    # Transport Code Options Array
    "transportCodeOptions": pl.List(pl.Struct({
        "urgency": pl.Int32,
        "transportCodes": pl.List(pl.Int32)
    })),
    
    # Customer Groups Array (legacy string array)
    "customerGroups": pl.List(pl.String),
    
    # Forwarders Array
    "forwarders": pl.List(pl.Struct({
        "id": pl.String,
        "description": pl.String,
        "trackingUrl": pl.String
    })),
    
    "defaultTimeZoneInfoId": pl.String,
    
    # Customer Group List Array (structured objects)
    "customerGroupList": pl.List(pl.Struct({
        "id": pl.String,
        "name": pl.String
    })),
    
    # Cosmos DB Metadata fields
    "_rid": pl.String,
    "_self": pl.String,
    "_etag": pl.String,
    "_attachments": pl.String,
    "_ts": pl.Int64
}