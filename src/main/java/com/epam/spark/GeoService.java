package com.epam.spark;

import ch.hsr.geohash.GeoHash;
import com.byteowls.jopencage.JOpenCageGeocoder;
import com.byteowls.jopencage.model.JOpenCageForwardRequest;
import com.byteowls.jopencage.model.JOpenCageLatLng;
import com.byteowls.jopencage.model.JOpenCageResponse;
import org.apache.http.HttpStatus;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Class for getting additional geo information
 */
public class GeoService implements Serializable {

    /**
     * Invokes Opencage API to receive latitude and longitude by given information about the place
     *
     * @param name    place name
     * @param country place country
     * @param city    place city
     * @param address place address
     * @return list with coordinates where latitude is at index 0 and longitude is at index 1.
     * Empty list in case of unsuccessful response from Opencage
     */
    public List<String> getCoordinates(String name, String country, String city, String address) {
        String apiKey ="ba288f1b83d24c17b6039e35135a00c5";
        JOpenCageGeocoder geocoder = new JOpenCageGeocoder(apiKey);

        String placeQuery = String.format("%s, %s, %s, %s", name, address, city, country);
        JOpenCageForwardRequest request = new JOpenCageForwardRequest(placeQuery);

        JOpenCageResponse response = geocoder.forward(request);
        if (response != null && response.getStatus().getCode() == HttpStatus.SC_OK) {
            JOpenCageLatLng firstResult = response.getFirstPosition();
            return Arrays.asList(firstResult.getLat().toString(), firstResult.getLng().toString());
        }
        return Collections.emptyList();
    }

    /**
     * Calculates geohash by given latitude and longitude
     * @return geohash string in Base32 containing 4 characters
     */
    public String getGeohash(Double latitude, Double longitude) {
        GeoHash geoHash = GeoHash.withCharacterPrecision(
                latitude,
                longitude,
                4);
        return geoHash.toBase32();
    }
}