/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.partition.strategy;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.internal.serialization.impl.GenericRecordQueryReader;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.util.PartitioningStrategyUtil;
import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.query.impl.getters.JsonGetter;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;

@SerializableByConvention
public final class AttributePartitioningStrategy implements PartitioningStrategy<Object> {

    private final String[] attributes;

    public AttributePartitioningStrategy(String... attributes) {
        checkAttributesAreValid(attributes);
        this.attributes = attributes;
    }

    @Override
    public Object getPartitionKey(final Object key) {
        final Object[] result;

        if (key instanceof InternalGenericRecord) {
            result = extractFromGenericRecord((InternalGenericRecord) key);
        } else if (key instanceof HazelcastJsonValue) {
            result = extractFromJson(key);
        } else if (key instanceof DataSerializable || key instanceof Serializable) {
            result = extractFromPojo(key);
        } else {
            throw new HazelcastException("Cannot extract attributes from the key");
        }

        return PartitioningStrategyUtil.constructAttributeBasedKey(result);
    }

    @Nonnull
    private Object[] extractFromPojo(final Object key) {
        final Object[] values = new Object[attributes.length];
        for (int i = 0; i < attributes.length; i++) {
            final String attribute = attributes[i];
            final Object value = ReflectionUtils.getFieldValue(attribute, key);
            checkNull(value, attribute);
            values[i] = value;
        }

        return values;
    }

    @Nonnull
    private Object[] extractFromJson(final Object key) {
        final Object[] values = new Object[attributes.length];
        for (int i = 0; i < attributes.length; i++) {
            final Object value = JsonGetter.INSTANCE.getValue(key, attributes[i]);
            checkNull(value, attributes[i]);
            values[i] = value;
        }

        return values;
    }

    @Nonnull
    private Object[] extractFromGenericRecord(final InternalGenericRecord key) {
        final Object[] values = new Object[attributes.length];
        final GenericRecordQueryReader reader = new GenericRecordQueryReader(key);
        for (int i = 0; i < attributes.length; i++) {
            final String attribute = attributes[i];
            try {
                final Object value = reader.read(attribute);
                checkNull(value, attributes[i]);
                values[i] = value;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return values;
    }

    public String[] getPartitioningAttributes() {
        return attributes;
    }

    private static void checkNull(Object extracted, String attributeName) {
        if (extracted == null) {
            throw new HazelcastException("Cannot extract '" + attributeName + "' from the key");
        }
    }

    private static void checkAttributesAreValid(final String[] attributes) {
        if (attributes == null) {
            throw new NullPointerException("Attributes names array must not be null.");
        }

        if (attributes.length == 0) {
            throw new IllegalArgumentException("Attributes names array must not be empty.");
        }

        for (final String attribute : attributes) {
            if (attribute == null || attribute.isEmpty()) {
                throw new IllegalArgumentException("Attribute name must not be empty.");
            }
        }
    }
}
