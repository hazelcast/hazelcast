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

package com.hazelcast.internal.serialization.impl.compact.record;

import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.compact.CompactReader;

import java.lang.reflect.Constructor;

/**
 * Constructs a record object by reading each of its components
 * one by one and passing them to its constructor.
 */
public final class JavaRecordReader {

    private final Constructor<?> recordConstructor;
    private final ComponentReaderWriter[] componentReaderWriters;

    public JavaRecordReader(Constructor<?> recordConstructor, ComponentReaderWriter[] componentReaderWriters) {
        this.recordConstructor = recordConstructor;
        this.componentReaderWriters = componentReaderWriters;
    }

    public Object readRecord(CompactReader compactReader, Schema schema) {
        Object[] components = new Object[componentReaderWriters.length];

        try {
            for (int i = 0; i < componentReaderWriters.length; i++) {
                components[i] = componentReaderWriters[i].readComponent(compactReader, schema);
            }
            return recordConstructor.newInstance(components);
        } catch (Exception e) {
            throw new HazelcastSerializationException("Failed to read the Java record", e);
        }
    }
}
