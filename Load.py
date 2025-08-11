def load_data_in_db(schema,table_name,load_df):
    load_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://localhost:3306/{schema}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", "root") \
        .option("password", "******") \
        .mode("append") \
        .save()
    print(f"✅ Loading {load_df.count()} rows to {schema}.{table_name}")