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

import com.hazelcast.nio.serialization.compact.CompactRecord;
import com.hazelcast.nio.serialization.compact.CompactRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.compact.TypeID;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

public class DeserializedCompactRecordCloner extends AbstractCompactRecordBuilder {
    private final TreeMap<String, Object> objects;
    private final Schema schema;
    private final Set<String> overwrittenFields = new HashSet<>();

    public DeserializedCompactRecordCloner(Schema schema, TreeMap<String, Object> objects) {
        this.objects = objects;
        this.schema = schema;
    }

    @Nonnull
    @Override
    public CompactRecord build() {
        return new DeserializedCompactRecord(schema, objects);
    }


    protected CompactRecordBuilder write(@Nonnull String fieldName, Object value, TypeID typeID) {
        checkTypeWithSchema(schema, fieldName, typeID);
        if (!overwrittenFields.add(fieldName)) {
            throw new HazelcastSerializationException("Field can only be written once");
        }
        objects.put(fieldName, value);
        return this;
    }
}
