package com.hazelcast.config;

import com.hazelcast.core.HazelcastException;

/**
 * A {@link HazelcastException} that is thrown when something is wrong with the server or client configuration.
 */
public class ConfigurationException extends HazelcastException {

    public ConfigurationException(String itemName, String candidate, String duplicate) {
        super(String
                .format("Found ambiguous configurations for item \"%s\": \"%s\" vs. \"%s\"%nPlease specify your configuration.",
                        itemName, candidate, duplicate));
    }
}
