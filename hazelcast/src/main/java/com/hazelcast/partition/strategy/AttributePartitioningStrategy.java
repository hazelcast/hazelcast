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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.internal.serialization.impl.GenericRecordQueryReader;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.query.impl.getters.JsonGetter;

import java.io.IOException;
import java.io.Serializable;

@SerializableByConvention
public class AttributePartitioningStrategy implements PartitioningStrategy {

    private String[] attributes;

    public AttributePartitioningStrategy(String... attributes) {
        this.attributes = attributes;
    }

    @Override
    public Object getPartitionKey(final Object key) {
        final Object[] values = new Object[attributes.length];
        if (key instanceof InternalGenericRecord) {
            if (!extractFromGenericRecord((InternalGenericRecord) key, values)) {
                return null;
            }
        } else if (key instanceof HazelcastJsonValue) {
            if (!extractFromJson(key, values)) {
                return null;
            }
        } else if (key instanceof DataSerializable || key instanceof Serializable) {
            if (!extractFromPojo(key, values)) {
                return null;
            }
        } else {
            // revert to default strategy for Portable and Compact POJOs
            return null;
        }

        return values;
    }

    private boolean extractFromPojo(final Object key, final Object[] values) {
        for (int i = 0; i < attributes.length; i++) {
            final String attribute = attributes[i];
            final Object value = ReflectionUtils.getFieldValue(attribute, key);
            if (value == null) {
                return false;
            }
            values[i] = value;
        }
        return true;
    }

    private boolean extractFromJson(final Object key, final Object[] values) {
        final Object[] extractedValues = JsonGetter.INSTANCE.getValues(key, attributes);
        for (int i = 0; i < extractedValues.length; i++) {
            final Object value = extractedValues[i];
            if (value == null) {
                return false;
            }
        }
        return true;
    }

    private boolean extractFromGenericRecord(final InternalGenericRecord key, final Object[] values) {
        final GenericRecordQueryReader reader = new GenericRecordQueryReader(key);
        for (int i = 0; i < attributes.length; i++) {
            final String attribute = attributes[i];
            try {
                final Object value = reader.read(attribute);
                if (value == null) {
                    return false;
                }
                values[i] = value;
            } catch (IOException exception) {
                return false;
            }
        }

        return true;
    }

    public String[] getPartitioningAttributes() {
        return attributes;
    }
}
