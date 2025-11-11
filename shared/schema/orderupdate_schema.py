orderupdate_schema = """{
            orderNumber: 'VARCHAR',
            orderDate: 'TIMESTAMP',
            salesBrand: 'VARCHAR',
            country: 'VARCHAR',
            currency: 'VARCHAR',

            source: 'STRUCT(name VARCHAR, title VARCHAR, isExternal BOOLEAN)',
            clientIP: 'VARCHAR',

            status: 'STRUCT(value VARCHAR, title VARCHAR)',
            fraudStatus: 'STRUCT(value VARCHAR, title VARCHAR)',
            deliveryStatus: 'STRUCT(value VARCHAR, title VARCHAR)',

            customerNumber: 'VARCHAR',
            civicNumber: 'VARCHAR',

            invoiceAddress: 'STRUCT(
                firstName VARCHAR,
                lastName VARCHAR,
                address STRUCT(street VARCHAR, postalCode VARCHAR, city VARCHAR, countryCode VARCHAR),
                email VARCHAR,
                mobilePhone VARCHAR
            )',

            buckets: 'STRUCT(
                bucketNumber INTEGER,
                returnCode VARCHAR,
                deliveredFrom STRUCT(name VARCHAR),
                deliveryAddress STRUCT(
                    firstName VARCHAR,
                    lastName VARCHAR,
                    address STRUCT(street VARCHAR, postalCode VARCHAR, city VARCHAR, countryCode VARCHAR),
                    email VARCHAR,
                    mobilePhone VARCHAR
                ),
                pickupPoint STRUCT(
                    id VARCHAR,
                    name VARCHAR,
                    address STRUCT(street VARCHAR, postalCode VARCHAR, city VARCHAR, countryCode VARCHAR)
                ),
                deliveryEstimate STRUCT(date DATE, text VARCHAR),
                shipping STRUCT(
                    carrier STRUCT(code VARCHAR, name VARCHAR),
                    method VARCHAR,
                    title VARCHAR,
                    fee DOUBLE,
                    returnFee DOUBLE,
                    unclaimedFee DOUBLE,
                    options JSON[]
                ),
                rows STRUCT(
                    rowNumber INTEGER,
                    sku VARCHAR,
                    name VARCHAR,
                    color VARCHAR,
                    size VARCHAR,
                    brand VARCHAR,
                    url VARCHAR,
                    imageName VARCHAR,
                    orderedQuantity INTEGER,
                    deliveredQuantity INTEGER,
                    cancelledQuantity INTEGER,
                    notifiedReturnQuantity INTEGER,
                    returnedQuantity INTEGER,
                    singleBasePrice DOUBLE,
                    singleSalesPrice DOUBLE,
                    offers STRUCT(
                        campaignId VARCHAR,
                        discount DOUBLE
                    )[],
                    manualDiscount DOUBLE,
                    totalDiscount DOUBLE,
                    totalAmount DOUBLE,
                    vatRate DOUBLE
                )[]
            )[]',

            offers: 'STRUCT(campaignId VARCHAR, title VARCHAR, activationCode VARCHAR, discount DOUBLE)[]',

            payments: 'STRUCT(
                title VARCHAR,
                psp VARCHAR,
                method VARCHAR,
                type VARCHAR,
                isTokenized BOOLEAN,
                alternative VARCHAR,
                amount DOUBLE,
                pspReference VARCHAR
            )[]',

            logs: 'JSON[]',

            total: 'STRUCT(
                handlingFee DOUBLE,
                shippingFee DOUBLE,
                salesAmount DOUBLE,
                discount DOUBLE,
                totalAmount DOUBLE,
                creditedAmount DOUBLE
            )',

            deliveries: 'STRUCT(
                deliveryNumber VARCHAR,
                deliveryDate DATE,
                carrier STRUCT(code VARCHAR, name VARCHAR),
                parcels STRUCT(
                    parcelNumber VARCHAR,
                    returnParcelNumber VARCHAR,
                    trackingLink VARCHAR,
                    parcelLink VARCHAR,
                    returnParcelLink VARCHAR,
                    weight DOUBLE,
                    volume DOUBLE,
                    rows STRUCT(
                        rowNumber INTEGER,
                        quantity DOUBLE
                    )[]
                )[]
            )[]',

            EventEnqueuedUtcTime: 'TIMESTAMP',
            SequenceNumber: 'UBIGINT',
            'Offset': 'VARCHAR',
            PartitionKey: 'VARCHAR'
        }"""