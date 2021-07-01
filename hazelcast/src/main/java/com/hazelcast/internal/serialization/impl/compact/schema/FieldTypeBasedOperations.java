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

package com.hazelcast.internal.serialization.impl.compact.schema;

import com.hazelcast.internal.serialization.impl.compact.DefaultCompactWriter;
import com.hazelcast.internal.serialization.impl.compact.InternalCompactRecord;
import com.hazelcast.nio.serialization.compact.CompactRecord;

import java.util.Objects;

/**
 * The purpose is to create on place to add new type to the serialization
 */
public interface FieldTypeBasedOperations {

    Object readObject(CompactRecord compactRecord, String fieldName);

    /**
     * For primitives serialized form is same as readObject.
     * This method will be overridden for Compact and will return CompactRecord representation of objects
     */
    default Object readInSerializedForm(CompactRecord compactRecord, String fieldName) {
        return readObject(compactRecord, fieldName);
    }

    default Object readIndexed(InternalCompactRecord record, String fieldName, int index) {
        throw new UnsupportedOperationException("Not an array type. Does not support read with an index");
    }


    default int hashCode(CompactRecord record, String fieldName) {
        return Objects.hashCode(readInSerializedForm(record, fieldName));
    }

    void readFromCompactRecordToWriter(DefaultCompactWriter defaultCompactWriter, CompactRecord compactRecord, String fieldName);
}
