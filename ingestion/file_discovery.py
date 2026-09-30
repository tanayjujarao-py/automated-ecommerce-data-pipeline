from datetime import datetime
from ingestion.snowflake_connection import get_snowflake_connection

def discover_files(pipeline):
    connection = get_snowflake_connection()
    cursor = connection.cursor()

    stage_name = pipeline["STAGE_NAME"]
    source_path = pipeline["SOURCE_PATH"]

    list_query = f"""
        LIST @{stage_name}/{source_path}
    """

    try:
        cursor.execute(list_query)
        rows = cursor.fetchall()
        files = []

        for row in rows:
            file_name = row[0]
            file_size = row[1]
            last_modified = row[3]

            # Convert Snowflake LIST timestamp string (e.g., Sat, 26 Sep 2026 11:21:54 GMT)
            last_modified_datetime = datetime.strptime(
                last_modified, "%a, %d %b %Y %H:%M:%S GMT"
            )

            files.append({
                "file_name": file_name,
                "file_size": file_size,
                "last_modified": last_modified_datetime,
            })

        return files
    finally:
        cursor.close()
        connection.close()