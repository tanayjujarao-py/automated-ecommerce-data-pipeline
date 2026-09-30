from ingestion.snowflake_connection import get_snowflake_connection

def get_active_pipelines():
    connection = get_snowflake_connection()
    cursor = connection.cursor()

    query = """
        SELECT 
            PIPELINE_ID,
            PIPELINE_NAME,
            SOURCE_TYPE,
            SOURCE_PATH,
            FILE_PATTERN,
            STAGE_NAME,
            TARGET_DATABASE,
            TARGET_SCHEMA,
            TARGET_TABLE,
            LOAD_TYPE
        FROM "ECOMMERCE-DATA-PIPELINE".METADATA.PIPELINE_CONFIG
        WHERE IS_ACTIVE = TRUE
        ORDER BY PIPELINE_ID
    """

    try:
        cursor.execute(query)
        column_names = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        pipelines = [dict(zip(column_names, row)) for row in rows]
        return pipelines
    finally:
        cursor.close()
        connection.close()