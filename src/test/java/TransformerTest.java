import com.epam.spark.GeoService;
import com.epam.spark.Transformer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import scala.collection.JavaConverters;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TransformerTest {
    private SparkSession session;

    private Transformer dataTransformer;
    private GeoService geoService;

    private StructType hotelSchema;
    private StructType weatherSchema;

    private StructType hotelSchemaWithGeohash;
    private StructType weatherSchemaWithGeohash;

    private StructType joinedDataSchema;

    @BeforeAll
    public void init() {
        session = SparkSession.builder().master("local[*]").getOrCreate();
        initHotelSchema();
        initWeatherSchema();
        initJoinedSchema();
        dataTransformer = new Transformer(session, new GeoService());
    }

    private void initHotelSchema() {
        hotelSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("Id", DataTypes.StringType, false),
                DataTypes.createStructField("Name", DataTypes.StringType, false),
                DataTypes.createStructField("Country", DataTypes.StringType, false),
                DataTypes.createStructField("City", DataTypes.StringType, false),
                DataTypes.createStructField("Address", DataTypes.StringType, false),
                DataTypes.createStructField("Latitude", DataTypes.StringType, true),
                DataTypes.createStructField("Longitude", DataTypes.StringType, true)
        });

        hotelSchemaWithGeohash = hotelSchema.add(DataTypes.createStructField("geohash", DataTypes.StringType, false));
    }

    private void initWeatherSchema() {
        weatherSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("avg_tmpr_c", DataTypes.StringType, false),
                DataTypes.createStructField("avg_tmpr_f", DataTypes.StringType, false),
                DataTypes.createStructField("lat", DataTypes.DoubleType, false),
                DataTypes.createStructField("lng", DataTypes.DoubleType, false),
                DataTypes.createStructField("wthr_date", DataTypes.StringType, false),
                DataTypes.createStructField("year", DataTypes.StringType, false),
                DataTypes.createStructField("month", DataTypes.StringType, false),
                DataTypes.createStructField("day", DataTypes.StringType, false)
        });

        weatherSchemaWithGeohash = weatherSchema.add(DataTypes.createStructField("geohash", DataTypes.StringType, false));
    }

    private void initJoinedSchema() {
        joinedDataSchema = DataTypes.createStructType(
                JavaConverters.seqAsJavaList(weatherSchemaWithGeohash.merge(hotelSchema))
                        .stream().filter(f -> !f.name().equals("Latitude") && !f.name().equals("Longitude"))
                        .collect(Collectors.toList()));
    }

    @Test
    public void addCoordinatesTest() {
        Dataset<Row> initial = initHotelsWithNullCoordinates(hotelSchema);
        Dataset<Row> expected = initHotels(hotelSchema);
        Dataset<Row> withCoordinates = dataTransformer.addCoordinatesToHotels(initial);
        checkEqual(withCoordinates, expected);
    }

    private Dataset<Row> initHotelsWithNullCoordinates(StructType schema) {
        return session.createDataset(Arrays.asList(
                RowFactory.create("1211180777473", "Sunset Inn", "US", "Tatum", "68 W Hwy Hwy 380", null, null, "9tym"),
                RowFactory.create("1108101562371", "Ihg", "US", "Metropolis", "2179 E 5th St","","","dn8g")
        ), RowEncoder.apply(schema));
    }

    private Dataset<Row> initHotels(StructType schema) {
        return session.createDataset(Arrays.asList(
                RowFactory.create("1211180777473", "Sunset Inn", "US", "Tatum", "68 W Hwy Hwy 380", "32.31599", "-94.51659", "9vsz"),
                RowFactory.create("1108101562371", "Ihg", "US", "Metropolis", "2179 E 5th St", "37.15117", "-88.732","dn8g")
        ), RowEncoder.apply(schema));
    }

    @Test
    public void addHotelGeohashTest() {
        Dataset<Row> initial = initHotels(hotelSchema);
        Dataset<Row> expected = initHotels(hotelSchemaWithGeohash);
        Dataset<Row> hotelsWithGeohash = dataTransformer.addGeohashToHotels(initial);
        hotelsWithGeohash.show();
        checkEqual(hotelsWithGeohash, expected);
    }

    @Test
    public void addWeatherGeohashTest() {
        Dataset<Row> initial = initWeather(weatherSchema);
        Dataset<Row> expected = initWeather(weatherSchemaWithGeohash);
        Dataset<Row> weatherWithGeohash = dataTransformer.addGeohashToWeather(initial);
        checkEqual(weatherWithGeohash, expected);
    }

    private Dataset<Row> initWeather(StructType schema) {
        return session.createDataset(Arrays.asList(
                RowFactory.create("17.7", "63.8", 37.13,-88.8975, "2016-10-01", "2016", "10", "1", "dn8g"),
                RowFactory.create("21.3", "70.4", 33.234, -103.706, "2016-10-03", "2016", "10", "3", "9tym")
        ), RowEncoder.apply(schema));
    }

    @Test
    public void joinTest() {
        Dataset<Row> hotels = initHotels(hotelSchemaWithGeohash);
        Dataset<Row> weather = initWeather(weatherSchemaWithGeohash);
        Dataset<Row> joined = dataTransformer.joinByGeohash(hotels, weather);
        Dataset<Row> expected = initJoined(joinedDataSchema);
        checkEqual(joined, expected);
    }

    private Dataset<Row> initJoined(StructType schema) {
        return session.createDataset(Arrays.asList(
                RowFactory.create("17.7", "63.8", 37.13, -88.8975, "2016-10-01", "2016", "10", "1", "dn8g",
                        "1108101562371", "Ihg", "US", "Metropolis", "2179 E 5th St"),
                RowFactory.create("21.3", "70.4", 33.234, -103.706, "2016-10-03", "2016", "10", "3", "9tym",
                        "1211180777473", "Sunset Inn", "US", "Tatum", "68 W Hwy Hwy 380")
        ), RowEncoder.apply(schema));
    }

    private void checkEqual(Dataset<Row> ds1, Dataset<Row> ds2) {
        List<Row> list1 = ds1.collectAsList();
        List<Row> list2 = ds2.collectAsList();
        Collection result = CollectionUtils.subtract(list1, list2);
        assertEquals(0, result.size());
    }

    @AfterAll
    public void stop() {
        session.stop();
    }
}
