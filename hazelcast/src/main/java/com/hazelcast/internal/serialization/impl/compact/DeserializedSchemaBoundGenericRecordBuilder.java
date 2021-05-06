/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import javax.annotation.Nonnull;
import java.util.TreeMap;

class DeserializedSchemaBoundGenericRecordBuilder extends AbstractGenericRecordBuilder {

    private final TreeMap<String, Object> objects = new TreeMap<>();
    private final Schema schema;

    DeserializedSchemaBoundGenericRecordBuilder(Schema schema) {
        this.schema = schema;
    }

    public @Nonnull
    GenericRecord build() {
        return new DeserializedGenericRecord(schema, objects);
    }

    @Override
    protected GenericRecordBuilder write(String fieldName, Object value, FieldType fieldType) {
        checkTypeWithSchema(schema, fieldName, fieldType);
        if (objects.putIfAbsent(fieldName, value) != null) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        return this;
    }
}
