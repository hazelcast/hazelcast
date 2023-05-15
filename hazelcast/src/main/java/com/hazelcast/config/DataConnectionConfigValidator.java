package com.hazelcast.config;

import com.hazelcast.spi.annotation.PrivateApi;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates an instance of {@link DataConnectionConfig}.
 */
@PrivateApi
public class DataConnectionConfigValidator {

    /**
     * Checks if this configuration object is in coherent state - has all required properties set.
     */
    @PrivateApi
    public static void validate(DataConnectionConfig conf) {
        List<String> errors = new ArrayList<>();
        if (conf.getName() == null || conf.getName().isEmpty()) {
            errors.add("Data connection name must be non-null and contain text");
        }
        if (conf.getType() == null || conf.getType().isEmpty()) {
            errors.add("Data connection type must be non-null and contain text");
        }
        if (conf.getProperties() == null) {
            errors.add("Data connection properties cannot be null, they can be empty");
        }
        if (!errors.isEmpty()) {
            throw new IllegalArgumentException(String.join(", ", errors));
        }
    }

}
