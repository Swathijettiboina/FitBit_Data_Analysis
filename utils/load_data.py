import os
import pandas as pd
import snowflake.connector
from io import StringIO

# Configuration
schema_name = "RAW_LAYER"
file_format_name = "raw_layer.my_csv_format"  
stage_base_path = "@raw_layer.FITBIT_DATA"

# Snowflake connection
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
        return 'TEXT'

try:
    # Get list of files in stage
    cs.execute(f"LIST {stage_base_path}")
    files_in_stage = cs.fetchall()
    
    for file_info in files_in_stage:
        file_path = file_info[0]  # Full stage path
        file_name = file_path.split('/')[-1]
        
        if file_name.endswith('.csv.gz'):
            base_name = os.path.splitext(os.path.splitext(file_name)[0])[0].lower().replace("-", "_")
            table_name = f"{base_name}"
            
            # Step 1: Download file content from stage to memory
            get_file_query = f"""
            SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10  # Adjust columns as needed
            FROM '{stage_base_path}/{file_name}'
            (FILE_FORMAT => '{file_format_name}')
            LIMIT 1000  # Sample rows for schema detection
            """
            cs.execute(get_file_query)
            sample_data = cs.fetchall()
            
            # Convert to pandas DataFrame
            df = pd.DataFrame(sample_data, columns=[desc[0] for desc in cs.description])
            
            # Step 2: Generate table schema from DataFrame
            column_defs = []
            for col in df.columns:
                snowflake_type = map_dtype_to_snowflake(df[col].dtype, col)
                column_defs.append(f'"{col}" {snowflake_type}')
            
            # Step 3: Create table
            create_query = f"""
            CREATE OR REPLACE TABLE {schema_name}.{table_name} (
                {',\n  '.join(column_defs)}
            );
            """
            print(f"Creating table: {schema_name}.{table_name}")
            cs.execute(create_query)
            
            # Step 4: Load full data from stage
            copy_query = f"""
            COPY INTO {schema_name}.{table_name}
            FROM '{stage_base_path}/{file_name}'
            FILE_FORMAT = '{file_format_name}';
            """
            print(f"Loading data into: {schema_name}.{table_name} from {file_name}")
            cs.execute(copy_query)
            
            print(f"✅ Done: {schema_name}.{table_name}\n")

finally:
    cs.close()
    conn.close()