from pyspark.sql.functions import col

# Remove duplicate transactions
dedup_df = df.dropDuplicates()

# Filter transactions with Amount > 200
filtered_query = """
SELECT * 
FROM transactions
WHERE Amount > 200
"""

# Execute the SQL query and get the result as a DataFrame
filtered_df = spark.sql(filtered_query)

# Save the transformed data to a Parquet file
output_path = "output/transformed_bank_transactions.parquet"
filtered_df.write.mode("overwrite").parquet(output_path)

print(f"Transformed data saved to {output_path}")