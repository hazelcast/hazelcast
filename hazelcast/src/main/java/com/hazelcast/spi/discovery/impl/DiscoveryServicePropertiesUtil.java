/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.discovery.impl;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.ValidationException;
import com.hazelcast.config.properties.ValueValidator;
import com.hazelcast.core.TypeConverter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

/**
 * Static utility class to validate, verify, and map Service Discovery related properties with the given definitions.
 */
final class DiscoveryServicePropertiesUtil {

    private DiscoveryServicePropertiesUtil() {
    }

    /**
     * Validates, verifies, and maps {@code properties} with the given {@code propertyDefinitions}.
     *
     * @param properties          properties from the Hazelcast node configuration (from the service-discovery section)
     * @param propertyDefinitions property definitions specific for the given
     *                            {@link com.hazelcast.spi.discovery.DiscoveryStrategy}
     * @return mapped properties
     * @throws InvalidConfigurationException if the the required properties are not satisfied or any property is not not
     *                                       applicable to the given definitions
     * @throws ValidationException           if any property is invalid
     */
    static Map<String, Comparable> prepareProperties(Map<String, Comparable> properties,
                                                     Collection<PropertyDefinition> propertyDefinitions) {
        Map<String, Comparable> mappedProperties = createHashMap(propertyDefinitions.size());

        for (PropertyDefinition propertyDefinition : propertyDefinitions) {
            String propertyKey = propertyDefinition.key();

            if (properties.containsKey(propertyKey.replace("-", ""))) {
                properties.put(propertyKey, properties.remove(propertyKey.replace("-", "")));
            }

            if (!properties.containsKey(propertyKey)) {
                if (!propertyDefinition.optional()) {
                    throw new InvalidConfigurationException(
                            String.format("Missing property '%s' on discovery strategy", propertyKey));
                }
                continue;
            }
            Comparable value = properties.get(propertyKey);

            TypeConverter typeConverter = propertyDefinition.typeConverter();
            Comparable mappedValue = typeConverter.convert(value);

            ValueValidator validator = propertyDefinition.validator();
            if (validator != null) {
                validator.validate(mappedValue);
            }

            mappedProperties.put(propertyKey, mappedValue);
        }

        verifyNoUnknownProperties(mappedProperties, properties);

        return mappedProperties;
    }

    private static void verifyNoUnknownProperties(Map<String, Comparable> mappedProperties,
                                                  Map<String, Comparable> allProperties) {
        Set<String> notMappedProperties = new HashSet<>(allProperties.keySet());
        notMappedProperties.removeAll(mappedProperties.keySet());
        if (!notMappedProperties.isEmpty()) {
            throw new InvalidConfigurationException(
                    String.format("Unknown properties: '%s' on discovery strategy", notMappedProperties));
        }
    }

}
