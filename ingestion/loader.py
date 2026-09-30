# File Location: ingestion/loader.py

from ingestion.snowflake_connection import get_snowflake_connection


def load_file(pipeline, file):
    connection = get_snowflake_connection()
    cursor = connection.cursor()

    target_database = pipeline["TARGET_DATABASE"]
    target_schema = pipeline["TARGET_SCHEMA"]
    target_table = pipeline["TARGET_TABLE"]

    stage_name = pipeline["STAGE_NAME"]

    # Strip full S3 URI prefix if present
    file_name = file["file_name"]
    raw_path = file_name.split("/")[-1]
    source_file = pipeline["SOURCE_PATH"]

    target_table_name = f'"{target_database}"."{target_schema}"."{target_table}"'
    source_file_path = f"{source_file}{raw_path}"

    if target_table == "BRONZE_CUSTOMERS":

        query = f"""
            COPY INTO {target_table_name}
            (
                CUSTOMER_ID,
                FIRST_NAME,
                LAST_NAME,
                EMAIL,
                PHONE,
                GENDER,
                DATE_OF_BIRTH,
                CITY,
                STATE,
                PINCODE,
                SIGNUP_DATE,
                CUSTOMER_SEGMENT,
                SOURCE_UPDATED_AT,
                INGESTED_AT,
                SOURCE_FILE 
            ) 
            FROM (
                SELECT 
                    $1,
                    $2,
                    $3,
                    $4,
                    $5,
                    $6,
                    $7,
                    $8,
                    $9,
                    $10,
                    $11,
                    $12,
                    $13,
                    CURRENT_TIMESTAMP(),
                    METADATA$FILENAME
                FROM @{stage_name}/{source_file_path}
            )
            FILE_FORMAT = (
                FORMAT_NAME = METADATA.CSV_FORMAT)
            """

    elif target_table == "BRONZE_PRODUCTS":

        query = f"""
            COPY INTO {target_table_name}
            (
                PRODUCT_ID,
                PRODUCT_NAME,
                CATEGORY,
                SUBCATEGORY,
                BRAND,
                UNIT_PRICE,
                COST_PRICE,
                SUPPLIER_ID,
                STOCK_QUANTITY,
                REORDER_LEVEL,
                PRODUCT_STATUS,
                INGESTED_AT,
                SOURCE_FILE
            ) 
            FROM (
                SELECT 
                    $1,
                    $2,
                    $3,
                    $4,
                    $5,
                    $6,
                    $7,
                    $8,
                    $9,
                    $10,
                    $11,
                    CURRENT_TIMESTAMP(),
                    METADATA$FILENAME
                FROM @{stage_name}/{source_file_path}
            )
            FILE_FORMAT = (
                FORMAT_NAME = METADATA.CSV_FORMAT)
            """

    elif target_table == "BRONZE_ORDERS":

        query = f"""
                COPY INTO {target_table_name}
                (
                    ORDER_ID,
                    CUSTOMER_ID,
                    PRODUCT_ID,
                    ORDER_DATE,
                    QUANTITY,
                    UNIT_PRICE,
                    DISCOUNT_AMOUNT,
                    SHIPPING_FEE,
                    TAX_AMOUNT,
                    ORDER_AMOUNT,
                    ORDER_STATUS,
                    SALES_CHANNEL,
                    PAYMENT_METHOD,
                    SHIPPING_CITY,
                    SHIPPING_STATE,
                    SOURCE_UPDATED_AT,
                    INGESTED_AT,
                    SOURCE_FILE
                ) 
                FROM (
                    SELECT 
                        $1,
                        $2,
                        $3,
                        $4,
                        $5,
                        $6,
                        $7,
                        $8,
                        $9,
                        $10,
                        $11,
                        $12,
                        $13,
                        $14,
                        $15,
                        $16,
                        CURRENT_TIMESTAMP(),
                        METADATA$FILENAME
                    FROM @{stage_name}/{source_file_path}
                )
                FILE_FORMAT = (
                    FORMAT_NAME = METADATA.CSV_FORMAT)
                """

    elif target_table == "BRONZE_PAYMENTS":

        query = f"""
                COPY INTO {target_table_name}
                (
                    PAYMENT_ID,
                    ORDER_ID,
                    PAYMENT_DATE,
                    PROCESSED_AT,
                    AMOUNT,
                    CURRENCY,
                    PAYMENT_STATUS,
                    GATEWAY,
                    FAILURE_REASON,
                    INGESTED_AT,
                    SOURCE_FILE
                ) 
                FROM (
                    SELECT 
                        $1,
                        $2,
                        $3,
                        $4,
                        $5,
                        $6,
                        $7,
                        $8,
                        $9,
                        CURRENT_TIMESTAMP(),
                        METADATA$FILENAME
                    FROM @{stage_name}/{source_file_path}
                )
                FILE_FORMAT = (
                    FORMAT_NAME = METADATA.CSV_FORMAT)
                """

    elif target_table == "BRONZE_RETURNS":

        query = f"""
                COPY INTO {target_table_name}
                (
                    RETURN_ID,
                    ORDER_ID,
                    PRODUCT_ID,
                    CUSTOMER_ID,
                    RETURN_DATE,
                    RETURN_QUANTITY,
                    RETURN_REASON,
                    REFUND_AMOUNT,
                    RETURN_STATUS,
                    REFUND_STATUS,
                    INGESTED_AT,
                    SOURCE_FILE
                ) 
                FROM (
                    SELECT 
                        $1,
                        $2,
                        $3,
                        $4,
                        $5,
                        $6,
                        $7,
                        $8,
                        $9,
                        $10,
                        CURRENT_TIMESTAMP(),
                        METADATA$FILENAME
                    FROM @{stage_name}/{source_file_path}
                )
                FILE_FORMAT = (
                    FORMAT_NAME = METADATA.CSV_FORMAT)
                """

    else:
        raise ValueError(f"Unsupported target table: {target_table}")

    try:
        print(f"Loading file relative path: {source_file_path}")
        print(f"Target table: {target_table_name}")
        cursor.execute(query)
        results = cursor.fetchall()
        return results
    finally:
        cursor.close()
        connection.close()
