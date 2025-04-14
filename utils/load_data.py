import os
import csv
import snowflake.connector

# === Configuration ===
base_paths = {
    "_3_12": r"C:\Users\HP\Documents\JMAN Training\DBT\FitBit\Fitabase Data 3.12.16-4.11.16",
    "_4_12": r"C:\Users\HP\Documents\JMAN Training\DBT\FitBit\Fitabase Data 4.12.16-5.12.16"
}

schema_name = "RAW"
file_format_name = "staging.my_csv_format"  # Correct file format name
stage_base_path = "@staging.FITBIT_STAGE"

conn = snowflake.connector.connect(
    user='SWATHIJETTIBOINA',
    password='Venky@123Venky@123',
    account='RTSSRMI-GWB24673',
    warehouse='COMPUTE_WH',
    database='FITBIT',
    schema=schema_name
)
cs = conn.cursor()

try:
    for suffix, folder_path in base_paths.items():
        for file in os.listdir(folder_path):
            if file.endswith(".csv"):
                file_path = os.path.join(folder_path, file)
                base_name = os.path.splitext(file)[0].lower().replace("-", "_")
                table_name = f"{base_name}{suffix}"

                # Step 1: Read headers from local CSV
                with open(file_path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    headers = next(reader)

                # Step 2: Create table in RAW schema
                column_defs = ",\n  ".join([f'"{col}" TEXT' for col in headers])
                create_query = f"""
                CREATE OR REPLACE TABLE {schema_name}.{table_name} (
                  {column_defs}
                );
                """
                print(f"🛠️ Creating table: {schema_name}.{table_name}")
                cs.execute(create_query)

                # Step 3: Copy data from Snowflake stage
                staged_file = f"{stage_base_path}/folder{suffix}/{file}"
                copy_query = f"""
                COPY INTO {schema_name}.{table_name}
                FROM '{staged_file}'
                FILE_FORMAT = '{file_format_name}';  -- Correct syntax here
                """
                print(f"📥 Loading data into: {schema_name}.{table_name} from {staged_file}")
                cs.execute(copy_query)

                print(f"✅ Done: {schema_name}.{table_name}\n")

finally:
    cs.close()
    conn.close()
