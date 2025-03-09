import snowflake.connector

# Snowflake connection details
SNOWFLAKE_ACCOUNT = "your_account_name"
SNOWFLAKE_USER = "your_username"
SNOWFLAKE_PASSWORD = "your_password"
SNOWFLAKE_WAREHOUSE = "your_warehouse"
SNOWFLAKE_DATABASE = "your_database"
SNOWFLAKE_SCHEMA = "your_schema"

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)

cursor = conn.cursor()

# Create stored procedure
create_proc_query = """
CREATE OR REPLACE PROCEDURE insert_data(IN id INT, IN name STRING, IN age INT)
RETURNS STRING
LANGUAGE SQL
AS 
$$
BEGIN
    INSERT INTO your_schema.your_table_name (id, name, age) 
    VALUES (id, name, age);
    RETURN 'Data inserted successfully';
END;
$$;
"""
cursor.execute(create_proc_query)
print("Stored Procedure Created Successfully.")

# Insert data using stored procedure
data = [
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Charlie", 35)
]

for row in data:
    cursor.execute(f"CALL insert_data({row[0]}, '{row[1]}', {row[2]})")

print("Data inserted successfully.")

# Verify inserted data
cursor.execute("SELECT * FROM your_schema.your_table_name;")
rows = cursor.fetchall()

print("Fetched Data:")
for row in rows:
    print(row)

# Close connection
cursor.close()
conn.close()
