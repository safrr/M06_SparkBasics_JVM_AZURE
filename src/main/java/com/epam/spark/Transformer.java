package com.epam.spark;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.util.List;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.column;

/**
  * Class for performing transformations on hotel and weather datasets
 */
public class Transformer implements Serializable {
    private final SparkSession session;
    private final GeoService geoService;

    public Transformer(SparkSession session, GeoService geoService) {
        this.session = session;
        this.geoService = geoService;
        registerGeohashUdf();
    }

    /**
     * Registers a new UDF that invokes {@link GeoService#getGeohash}
     * with provided latitude and longitude
     */
    private void registerGeohashUdf() {
        session.udf().register("geohashUDF",
                (UDF2<Double, Double, String>) geoService::getGeohash, DataTypes.StringType);
    }

     /**
      * Add latitude and longitude for hotels if it is needed.
     */
    public Dataset<Row> addCoordinatesToHotels(Dataset<Row> hotelDs) {
        ExpressionEncoder<Row> hotelEncoder = RowEncoder.apply(hotelDs.schema());
        return hotelDs.map((MapFunction<Row, Row>) this::getHotelWithCoordinates, hotelEncoder);
    }

     /**
      * Invokes {@link GeoService#getCoordinates} if latitude or longitude are not specified
      * and returns a new row with received coordinates. Otherwise, returns the given row without changes
      * @param row hotel row
      * @return row with populated latitude and longitude
     */
    private Row getHotelWithCoordinates(Row row) {
        String latitude = row.getAs("Latitude");
        String longitude = row.getAs("Longitude");
        String id = row.getAs("Id");
        String name = row.getAs("Name");
        String country = row.getAs("Country");
        String city = row.getAs("City");
        String address = row.getAs("Address");
        if (coordinatesNotDefined(latitude, longitude)) {
            List<String> coordinates = geoService.getCoordinates(name, country, city, address);
            return RowFactory.create(id, name, country, city, address,
                    coordinates.get(0), coordinates.get(1));
        }
        return row;
    }

    /**
     * Check coordinates values
     *
     * @param latitude    place latitude
     * @param longitude place longitude
     * @return true if coordinates not defined
     */
    private boolean coordinatesNotDefined(String latitude, String longitude) {
        return StringUtils.equals(latitude, "NA") ||
                StringUtils.equals(longitude, "NA") ||
                StringUtils.isBlank(latitude) ||
                StringUtils.isBlank(longitude);
    }

    /**
     * Adds new column with geohash value to the hotel dataset
     */
    public Dataset<Row> addGeohashToHotels(Dataset<Row> hotelDs) {
        return hotelDs
                .withColumn("geohash",
                        callUDF("geohashUDF",
                                column("latitude").cast(DataTypes.DoubleType),
                                column("longitude").cast(DataTypes.DoubleType)));
    }

    /**
     * Adds new column with geohash value to the weather dataset
     */
    public Dataset<Row> addGeohashToWeather(Dataset<Row> weatherDs) {
        return weatherDs.withColumn(
                "geohash",
                callUDF("geohashUDF", column("lat"), column("lng")))
                .dropDuplicates("year", "month", "day", "geohash");
    }

     /**
      * Left join weather with hotels dropping duplicate columns
     */
    public Dataset<Row> joinByGeohash(Dataset<Row> hotelsDs, Dataset<Row> weatherDs) {
        return weatherDs.join(
                hotelsDs, hotelsDs.col("geohash").equalTo(weatherDs.col("geohash")), "inner")
                .drop(hotelsDs.col("geohash"))
                .drop(hotelsDs.col("Latitude"))
                .drop(hotelsDs.col("Longitude"));
    }
}
