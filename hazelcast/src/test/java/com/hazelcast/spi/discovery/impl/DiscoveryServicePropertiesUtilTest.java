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
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.config.properties.ValidationException;
import com.hazelcast.config.properties.ValueValidator;
import com.hazelcast.core.TypeConverter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.config.properties.PropertyTypeConverter.STRING;
import static com.hazelcast.spi.discovery.impl.DiscoveryServicePropertiesUtil.prepareProperties;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DiscoveryServicePropertiesUtilTest {

    private static final String PROPERTY_KEY_1 = "property1";
    private static final String PROPERTY_VALUE_1 = "propertyValue1";
    private static final PropertyDefinition PROPERTY_DEFINITION_1 = new SimplePropertyDefinition(PROPERTY_KEY_1, STRING);

    @Test
    public void correctProperties() {
        // given
        Map<String, Comparable> properties = new HashMap<>(singletonMap(PROPERTY_KEY_1, (Comparable) PROPERTY_VALUE_1));
        Collection<PropertyDefinition> propertyDefinitions = singletonList(PROPERTY_DEFINITION_1);

        // when
        Map<String, Comparable> result = prepareProperties(properties, propertyDefinitions);

        // then
        assertEquals(PROPERTY_VALUE_1, result.get(PROPERTY_KEY_1));
    }

    @Test
    public void correctDashlessPropertyConversion() {
        // given
        Map<String, Comparable> properties = new HashMap<>(singletonMap("customproperty", PROPERTY_VALUE_1));
        Collection<PropertyDefinition> propertyDefinitions = singletonList(new SimplePropertyDefinition("custom-property", STRING));

        // when
        Map<String, Comparable> result = prepareProperties(properties, propertyDefinitions);

        // then
        assertEquals(PROPERTY_VALUE_1, result.get("custom-property"));
    }

    @Test(expected = InvalidConfigurationException.class)
    public void unsatisfiedRequiredProperty() {
        // given
        Map<String, Comparable> properties = emptyMap();
        Collection<PropertyDefinition> propertyDefinitions = singletonList(PROPERTY_DEFINITION_1);

        // when
        prepareProperties(properties, propertyDefinitions);

        // then
        // throw exception
    }

    @Test
    public void unsatisfiedOptionalProperty() {
        // given
        Map<String, Comparable> properties = emptyMap();
        Collection<PropertyDefinition> propertyDefinitions = singletonList(
                (PropertyDefinition) new SimplePropertyDefinition(PROPERTY_KEY_1, true, STRING));

        // when
        Map<String, Comparable> result = prepareProperties(properties, propertyDefinitions);

        // then
        assertTrue(result.isEmpty());
    }

    @Test(expected = ValidationException.class)
    public void invalidProperty() {
        // given
        Map<String, Comparable> properties = new HashMap<>(singletonMap(PROPERTY_KEY_1, (Comparable) PROPERTY_VALUE_1));

        ValueValidator<String> valueValidator = mock(ValueValidator.class);
        willThrow(new ValidationException("Invalid property")).given(valueValidator).validate(PROPERTY_VALUE_1);
        Collection<PropertyDefinition> propertyDefinitions = singletonList(
                (PropertyDefinition) new SimplePropertyDefinition(PROPERTY_KEY_1, false, STRING, new DummyValidator()));

        // when
        prepareProperties(properties, propertyDefinitions);

        // then
        // throw exception
    }

    @Test(expected = InvalidConfigurationException.class)
    public void unknownProperty() {
        // given
        Map<String, Comparable> properties = new HashMap<>(singletonMap(PROPERTY_KEY_1, (Comparable) PROPERTY_VALUE_1));
        Collection<PropertyDefinition> propertyDefinitions = emptyList();

        // when
        prepareProperties(properties, propertyDefinitions);

        // then
        // throw exception
    }

    @Test
    public void nullProperty() {
        // given
        Map<String, Comparable> properties = new HashMap<>(singletonMap(PROPERTY_KEY_1, null));
        TypeConverter typeConverter = new TypeConverter() {
            @Override
            public Comparable convert(Comparable value) {
                return value == null ? "hazel" : "cast";
            }
        };
        Collection<PropertyDefinition> propertyDefinitions = singletonList(
                (PropertyDefinition) new SimplePropertyDefinition(PROPERTY_KEY_1, true, typeConverter));

        // when
        Map<String, Comparable> result = prepareProperties(properties, propertyDefinitions);

        // then
        assertEquals("hazel", result.get(PROPERTY_KEY_1));
    }

    private static class DummyValidator implements ValueValidator<String> {

        @Override
        public void validate(String value)
                throws ValidationException {
            throw new ValidationException("Invalid property");
        }
    }
}
