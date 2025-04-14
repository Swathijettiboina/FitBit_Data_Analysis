import snowflake.connector

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='SWATHIJETTIBOINA',
    password='Venky@123Venky@123',
    account='RTSSRMI-GWB24673',
    warehouse='COMPUTE_WH',
    database='FITBIT',
    schema='STAGING'
)
cs = conn.cursor()

try:
    # Fetch all table names from STAGING schema
    cs.execute("""
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'STAGING' AND TABLE_TYPE = 'BASE TABLE';
    """)
    tables = cs.fetchall()

    # Drop each table
    for table in tables:
        table_name = table[0]
        print(f"Dropping table: STAGING.{table_name}")
        cs.execute(f'DROP TABLE IF EXISTS STAGING."{table_name}"')

    print("✅ All tables in STAGING dropped.")

finally:
    cs.close()
    conn.close()
