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
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

public class DeserializedGenericRecordCloner extends AbstractGenericRecordBuilder {
    private final TreeMap<String, Object> objects;
    private final Schema schema;
    private final Set<String> overwrittenFields = new HashSet<>();

    public DeserializedGenericRecordCloner(Schema schema, TreeMap<String, Object> objects) {
        this.objects = objects;
        this.schema = schema;
    }

    @Nonnull
    @Override
    public GenericRecord build() {
        return new DeserializedGenericRecord(schema, objects);
    }


    protected GenericRecordBuilder write(@Nonnull String fieldName, Object value, FieldType fieldType) {
        checkTypeWithSchema(schema, fieldName, fieldType);
        if (!overwrittenFields.add(fieldName)) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        objects.put(fieldName, value);
        return this;
    }
}
