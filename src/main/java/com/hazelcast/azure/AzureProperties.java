/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.config.properties.ValidationException;
import com.hazelcast.config.properties.ValueValidator;

import static com.hazelcast.config.properties.PropertyTypeConverter.INTEGER;
import static com.hazelcast.config.properties.PropertyTypeConverter.STRING;

import java.util.Map;

/**
 *  Defines the name and default value for JCloud properties
 */
public final class AzureProperties {

    /**
     * Unique identifier for ComputeService Provider
     * see the full list of ids : https://jclouds.apache.org/reference/providers/
     */
    public static final PropertyDefinition CLIENT_ID = property("client-id", STRING);
    /**
     * Unique credential identity specific to users cloud account
     */
    public static final PropertyDefinition TENANT_ID = property("tenant-id", STRING);
    /**
     * Unique credential specific to users cloud accounts identity
     */
    public static final PropertyDefinition SUBSCRIPTION_ID = property("subscription-id", STRING);
    /**
     * Property used to define zones for node filtering
     */
    public static final PropertyDefinition CLIENT_SECRET = property("client-secret", STRING);

    public static final PropertyDefinition HZLCST_CLUSTER_ID = property("hzlcst-cluster-id", STRING);
    /**
     * Resouce group to scan for virtual machines
     */
    public static final PropertyDefinition GROUP_NAME = property("group-name", STRING);

    private AzureProperties() {
    }

    private static PropertyDefinition property(String key, PropertyTypeConverter typeConverter) {
        return property(key, typeConverter, null);
    }

    private static PropertyDefinition property(String key, PropertyTypeConverter typeConverter,
                                               ValueValidator valueValidator) {
        return new SimplePropertyDefinition(key, true, typeConverter, valueValidator);
    }

    private static final int MIN_PORT = 0;
    private static final int MAX_PORT = 65535;

    /**
     * Validator for valid network ports
     */
    protected static class PortValueValidator implements ValueValidator<Integer> {

        public void validate(Integer value) throws ValidationException {
            if (value < MIN_PORT) {
                throw new ValidationException("hz-port number must be greater 0");
            }
            if (value > MAX_PORT) {
                throw new ValidationException("hz-port number must be less or equal to 65535");
            }
        }
    }

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