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

public class GeoService implements Serializable {
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

    public String getGeohash(Double latitude, Double longitude) {
        GeoHash geoHash = GeoHash.withCharacterPrecision(
                latitude,
                longitude,
                4);
        return geoHash.toBase32();
    }
}
