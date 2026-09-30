from ingestion.snowflake_connection import get_snowflake_connection

def is_file_processed(pipeline_id, source_file):
    connection = get_snowflake_connection()
    cursor = connection.cursor()

    query = """
        SELECT COUNT(*)
        FROM "ECOMMERCE-DATA-PIPELINE".METADATA.INGESTION_LOG
        WHERE PIPELINE_ID = %s
          AND SOURCE_FILE = %s
          AND STATUS = 'SUCCESS'
    """

    try:
        cursor.execute(query, (pipeline_id, source_file))
        result = cursor.fetchone()
        return result[0] > 0
    finally:
        cursor.close()
        connection.close()

def write_ingestion_log(
    run_id,
    pipeline_id,
    source_file,
    file_size,
    file_last_modified,
    status,
    rows_loaded,
    start_time,
    end_time,
    error_message=None,
):
    connection = get_snowflake_connection()
    cursor = connection.cursor()

    query = """
        INSERT INTO "ECOMMERCE-DATA-PIPELINE".METADATA.INGESTION_LOG
        (
            RUN_ID,
            PIPELINE_ID,
            SOURCE_FILE,
            FILE_SIZE,
            FILE_LAST_MODIFIED,
            STATUS,
            ROWS_LOADED,
            START_TIME,
            END_TIME,
            ERROR_MESSAGE
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        cursor.execute(
            query,
            (
                run_id,
                pipeline_id,
                source_file,
                file_size,
                file_last_modified,
                status,
                rows_loaded,
                start_time,
                end_time,
                error_message,
            ),
        )
        connection.commit()
    finally:
        cursor.close()
        connection.close()