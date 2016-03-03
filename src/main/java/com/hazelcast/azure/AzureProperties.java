/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.azure;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.config.properties.ValidationException;
import com.hazelcast.config.properties.ValueValidator;

import java.util.Map;

import static com.hazelcast.config.properties.PropertyTypeConverter.STRING;

/**
 *  Defines the properties required by teh Azure SPI and the names used in the configuration
 *  Includes helpers for retrieving properties
 */
public final class AzureProperties {

    /**
     * The constant CLIENT_ID.
     */
    public static final PropertyDefinition CLIENT_ID = property("client-id", STRING);

    /**
     * The constant TENANT_ID.
     */
    public static final PropertyDefinition TENANT_ID = property("tenant-id", STRING);

    /**
     * The constant SUBSCRIPTION_ID.
     */
    public static final PropertyDefinition SUBSCRIPTION_ID = property("subscription-id", STRING);

    /**
     * The constant CLIENT_SECRET.
     */
    public static final PropertyDefinition CLIENT_SECRET = property("client-secret", STRING);

    /**
     * The constant CLUSTER_ID.
     */
    public static final PropertyDefinition CLUSTER_ID = property("cluster-id", STRING);

    /**
     * The constant GROUP_NAME.
     */
    public static final PropertyDefinition GROUP_NAME = property("group-name", STRING);

    private static final int MIN_PORT = 0;
    private static final int MAX_PORT = 65535;

    private AzureProperties() {
    }

    /**
     * Returns a property definition given a string and type converter
     *
     * @param key the key to use for the property
     * @param typeConverter the PropertyTypeConverter to convert the property
     * @return PropertyDefition the PropertyDefition for they key
     */
    private static PropertyDefinition property(String key, PropertyTypeConverter typeConverter) {
        return property(key, typeConverter, null);
    }

    /**
     * Returns a property definition given a string and type converter
     *
     * @param key the key to use for the property
     * @param typeConverter the PropertyTypeConverter to convert the property
     * @param valueValidator the validator for the key value
     * @return SimplePropertyDefinition the PropertyDefition for they key
     */
    private static PropertyDefinition property(String key, PropertyTypeConverter typeConverter,
                                               ValueValidator valueValidator) {
        return new SimplePropertyDefinition(key, true, typeConverter, valueValidator);
    }

    /**
     * Validator for valid network ports
     */
    public static class PortValueValidator implements ValueValidator<Integer> {

        /**
        * Returns a validation
        *
        * @param value the integer to validate
        * @throws ValidationException if value does not fall in valid port number range
        */
        public void validate(Integer value) throws ValidationException {
            if (value < MIN_PORT) {
                throw new ValidationException("hz-port number must be greater 0");
            }
            if (value > MAX_PORT) {
                throw new ValidationException("hz-port number must be less or equal to 65535");
            }
        }
    }

    /**
     * Returns a Comparable type for the specified property defition in the provided
     * property map
     *
     * @param property the PropertyDefition to use provided by
     * @param properties the properties map to retrieve the property from
     * @return the or null
     * @throws ValidationException if value does not fall in valid port number range
     */
    public static <T extends Comparable> T getOrNull(PropertyDefinition property, Map<String, Comparable> properties) {
        return getOrDefault(property, properties, null);
    }

    private static <T extends Comparable> T getOrDefault(PropertyDefinition property,
      Map<String, Comparable> properties, T defaultValue) {

        if (properties == null || property == null) {
            return defaultValue;
        }

        Comparable value = properties.get(property.key());
        if (value == null) {
            return defaultValue;
        }

        return (T) value;
    }
}
