package com.epam.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

public class Repository implements Serializable {
    public Dataset<Row> readCSV(SparkSession session, String path) {
        return session.read().option("header", "true").csv(path);
    }

    public Dataset<Row> readParquet(SparkSession session, String path) {
        return session.read().parquet(path);
    }

    public void writeParquet(Dataset<Row> ds, String path, String... partitionBy) {
        ds.write().partitionBy(partitionBy).mode("overwrite").parquet(path);
    }
}
