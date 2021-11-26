package com.epam.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Class for reading and writing data in CSV or Parquet
 */
public class Repository implements Serializable {

    /**
     * Reads CSV file from the given path
     * @param session initialized Spark session
     * @param path path from which to read CSV file
     * @return dataset of rows
     */
    public Dataset<Row> readCSV(SparkSession session, String path) {
        return session.read().option("header", "true").csv(path);
    }

    /**
     * Reads Parquet file from the given path
     * @param session initialized Spark session
     * @param path path from which to read Parquet file
     * @return dataset of rows
     */
    public Dataset<Row> readParquet(SparkSession session, String path) {
        return session.read().parquet(path);
    }

    /**
     * Writes Parquet file by the given path
     * @param ds dataset to write to the file
     * @param path path to write the file
     * @param partitionBy fields by which to partition the dataset
     */
    public void writeParquet(Dataset<Row> ds, String path, String... partitionBy) {
        ds.write().partitionBy(partitionBy).mode("overwrite").parquet(path);
    }
}
