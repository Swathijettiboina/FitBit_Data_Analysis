import os
import pandas as pd
import snowflake.connector

base_paths = {
    "_3_12": r"C:\Users\HP\Documents\JMAN Training\DBT\FitBit\Fitabase Data 3.12.16-4.11.16",
    "_4_12": r"C:\Users\HP\Documents\JMAN Training\DBT\FitBit\Fitabase Data 4.12.16-5.12.16"
}

schema_name = "RAW_LAYER"  # Store tables in RAW_LAYER schema
file_format_name = "staging.my_csv_format"  
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

def map_dtype_to_snowflake(dtype, col_name):
    if pd.api.types.is_integer_dtype(dtype):
        return 'NUMBER'
    elif pd.api.types.is_float_dtype(dtype):
        return 'NUMBER(38, 6)'
    else:
        return 'TEXT'  # Default to TEXT for other data types

try:
    for suffix, folder_path in base_paths.items():
        for file in os.listdir(folder_path):
            if file.endswith(".csv"):
                file_path = os.path.join(folder_path, file)
                base_name = os.path.splitext(file)[0].lower().replace("-", "_")
                table_name = f"{base_name}{suffix}"

                df = pd.read_csv(file_path, encoding='utf-8')

                column_defs = []
                for col in df.columns:
                    snowflake_type = map_dtype_to_snowflake(df[col].dtype, col)
                    column_defs.append(f'"{col}" {snowflake_type}')

                # Step 3: Create table in RAW_LAYER schema
                create_query = f"""
                CREATE OR REPLACE TABLE {schema_name}.{table_name} (
                  {',\n  '.join(column_defs)}
                );
                """
                print(f"Creating table: {schema_name}.{table_name}")
                cs.execute(create_query)

                # Step 4: Copy data from Snowflake stage
                staged_file = f"{stage_base_path}/folder{suffix}/{file}"
                copy_query = f"""
                COPY INTO {schema_name}.{table_name}
                FROM '{staged_file}'
                FILE_FORMAT = '{file_format_name}';  
                """
                print(f"Loading data into: {schema_name}.{table_name} from {staged_file}")
                cs.execute(copy_query)

                print(f"✅ Done: {schema_name}.{table_name}\n")

finally:
    cs.close()
    conn.close()
