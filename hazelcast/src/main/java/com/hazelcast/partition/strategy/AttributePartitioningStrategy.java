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

import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.internal.serialization.impl.GenericRecordQueryReader;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.partition.PartitioningStrategy;

import java.io.IOException;

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
            GenericRecordQueryReader reader = new GenericRecordQueryReader((InternalGenericRecord) key);
            for (int i = 0; i < attributes.length; i++) {
                final String attribute = attributes[i];
                try {
                    final Object value = reader.read(attribute);
                    if (value == null) {
                        return null;
                    }
                    values[i] = value;
                } catch (IOException exception) {
                    return null;
                }
            }
        } else {
            for (int i = 0; i < attributes.length; i++) {
                final String attribute = attributes[i];
                final Object value = ReflectionUtils.getFieldValue(attribute, key);
                if (value == null) {
                    return null;
                }
                values[i] = value;
            }
        }

        return values;
    }

    public String[] getPartitioningAttributes() {
        return attributes;
    }
}
