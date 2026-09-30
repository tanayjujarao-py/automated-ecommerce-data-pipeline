# File Location: ingestion/run_pipeline.py

import uuid
from datetime import datetime
from ingestion.metadata_reader import get_active_pipelines
from ingestion.file_discovery import discover_files
from ingestion.ingestion_log import is_file_processed, write_ingestion_log
from ingestion.loader import load_file


def extract_rows_loaded(results):
    """Safely extracts loaded row count from Snowflake COPY INTO output."""
    if not results:
        return 0

    first_row = results[0]

    # 1. Standard COPY INTO output (rows_loaded is at index 3)
    if len(first_row) > 3 and isinstance(first_row[3], int):
        return first_row[3]

    # 2. MATCH_BY_COLUMN_NAME output (rows_loaded is often at index 1 or 2)
    for val in first_row:
        if isinstance(val, int) and val > 0:
            return val

    return 0


def run_pipeline():
    run_id = str(uuid.uuid4())

    print("=" * 70)
    print("METADATA-DRIVEN E-COMMERCE PIPELINE STARTED")
    print(f"RUN ID: {run_id}")
    print("=" * 70)

    pipelines = get_active_pipelines()

    for pipeline in pipelines:
        pipeline_id = pipeline["PIPELINE_ID"]
        pipeline_name = pipeline["PIPELINE_NAME"]

        print("\n" + "=" * 70)
        print(f"PIPELINE: {pipeline_name}")
        print("=" * 70)

        files = discover_files(pipeline)

        if not files:
            print("No files found.")
            continue

        for file in files:
            source_file = file["file_name"]
            file_size = file["file_size"]
            file_last_modified = file["last_modified"]

            processed = is_file_processed(pipeline_id, source_file)

            if processed:
                print(f"SKIPPING: {source_file}")
                print("Reason: File was already successfully processed.")
                continue

            start_time = datetime.now()
            print(f"PROCESSING: {source_file}")

            try:
                results = load_file(pipeline, file)
                rows_loaded = extract_rows_loaded(results)
                end_time = datetime.now()

                if rows_loaded > 0:
                    status = "SUCCESS"
                    error_msg = None
                else:
                    status = "FAILED"
                    error_msg = f"COPY INTO executed but returned no loaded rows. Raw output: {results}"

                write_ingestion_log(
                    run_id=run_id,
                    pipeline_id=pipeline_id,
                    source_file=source_file,
                    file_size=file_size,
                    file_last_modified=file_last_modified,
                    status=status,
                    rows_loaded=rows_loaded,
                    start_time=start_time,
                    end_time=end_time,
                    error_message=error_msg,
                )

                if status == "SUCCESS":
                    print(f"SUCCESS: {source_file}")
                    print(f"ROWS LOADED: {rows_loaded}")
                else:
                    print(f"FAILED: {source_file}")
                    print(f"ERROR: {error_msg}")

            except Exception as error:
                end_time = datetime.now()
                print(f"FAILED: {source_file}")
                print(f"ERROR: {error}")

                write_ingestion_log(
                    run_id=run_id,
                    pipeline_id=pipeline_id,
                    source_file=source_file,
                    file_size=file_size,
                    file_last_modified=file_last_modified,
                    status="FAILED",
                    rows_loaded=0,
                    start_time=start_time,
                    end_time=end_time,
                    error_message=str(error),
                )

    print("\n" + "=" * 70)
    print("METADATA-DRIVEN PIPELINE FINISHED")
    print("=" * 70)


if __name__ == "__main__":
    run_pipeline()
