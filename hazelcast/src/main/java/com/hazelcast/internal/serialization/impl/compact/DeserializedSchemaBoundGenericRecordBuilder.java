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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.TreeMap;

/**
 * Builder that will be used while creating a new builder out of
 * {@link DeserializedGenericRecord}s. It carries the schema information
 * of the record and do type checks while setting fields.
 */
public class DeserializedSchemaBoundGenericRecordBuilder extends AbstractGenericRecordBuilder {

    private final TreeMap<String, Object> objects = new TreeMap<>();
    private final Schema schema;

    public DeserializedSchemaBoundGenericRecordBuilder(Schema schema) {
        this.schema = schema;
    }

    public @Nonnull
    GenericRecord build() {
        Set<String> fieldNames = schema.getFieldNames();
        for (String fieldName : fieldNames) {
            if (!objects.containsKey(fieldName)) {
                throw new HazelcastSerializationException("Found an unset field " + fieldName
                        + ". All the fields must be set before build");
            }
        }
        return new DeserializedGenericRecord(schema, objects);
    }

    @Override
    protected GenericRecordBuilder write(@Nonnull String fieldName, Object value, FieldKind fieldKind) {
        checkTypeWithSchema(schema, fieldName, fieldKind);
        if (objects.putIfAbsent(fieldName, value) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }
}
