
def extract_data_mysql(spark, schema, table_name, partition_column=None, lower_bound=None, upper_bound=None, num_partitions=None):
    reader = (spark.read
        .format("jdbc")
        .option(f"url", f"jdbc:mysql://localhost:3306/{schema}")
        .option("dbtable", table_name)
        .option("user", "root")
        .option("password", "******")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("fetchsize", "100000"))

    if partition_column and lower_bound and upper_bound and num_partitions:
        reader = (reader
                  .option("partitionColumn", partition_column)
                  .option("lowerBound", lower_bound)
                  .option("upperBound", upper_bound)
                  .option("numPartitions", num_partitions))

    extracted_df=reader.load()
    print(f"✅ Extracted {extracted_df.count()} rows from {schema}.{table_name}")
    return extracted_df