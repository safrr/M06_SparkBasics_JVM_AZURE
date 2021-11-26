package com.epam.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Properties;
public class App {
    public static void main(String[] args) {
        /**
         * Set spark conf,
         * Initializing spark session,
         * set app name and data to access ADLS
         */
        SparkConf conf = new SparkConf();
        conf.set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net","OAuth");
        conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
        conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net",
                "f3905ff9-16d4-43ac-9011-842b661d556d");
        conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net",
                "mAwIU~M4~xMYHi4YX_uT8qQ.ta2.LTYZxT");
        conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net",
                "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token");
        conf.set("fs.azure.account.key.stsparktask01.dfs.core.windows.net","gJb1/45vD3tuaBt+sla3OI+7Kj4leK1oG6k/KzJeruVBvRO7dQ6vJ2qJtpmyqPBtX6eeUzCPNCrkRfKtlU8fYw==");

        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .appName("SparkBasic")
                .config(conf)
                .getOrCreate();

        Properties properties = readProperties();
        Repository dataRepository = new Repository();
        Transformer dataTransformer = new Transformer(session, new GeoService());

        Dataset<Row> hotelDataset = dataRepository.readCSV(session, properties.getProperty("input.path.hotels"));

        Dataset<Row> weatherDataset = dataRepository.readParquet(session, properties.getProperty("input.path.weather"));

        Dataset<Row> hotelsWithCoordinates = dataTransformer.addCoordinatesToHotels(hotelDataset);

        Dataset<Row> hotelsWithGeohash = dataTransformer.addGeohashToHotels(hotelsWithCoordinates);

        Dataset<Row> weatherWithGeohash = dataTransformer.addGeohashToWeather(weatherDataset);

        Dataset<Row> joinedDataset = dataTransformer.joinByGeohash(hotelsWithGeohash, weatherWithGeohash);

        dataRepository.writeParquet(joinedDataset, properties.getProperty("output.path.result"), "year", "month", "day");

        session.stop();
    }

    /**
     * Read properties file
     * @return properties
     */
    private static Properties readProperties() {
        Properties properties = new Properties();
        try {
            properties.load(App.class.getClassLoader().getResourceAsStream("conf.properties"));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return properties;
    }
}
