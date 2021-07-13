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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.impl.compact.DefaultCompactWriter;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;

import java.util.Objects;

/**
 * The purpose is to create on place to add new type to the serialization
 */
public interface FieldTypeBasedOperations {

    int VARIABLE_SIZE = -1;

    Object readObject(GenericRecord genericRecord, String fieldName);

    /**
     * For primitives this will return boxed object.
     * This method will be overridden for Portable and Compact and will return GenericRecord representation of objects
     */
    default Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
        return readObject(genericRecord, fieldName);
    }

    default Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
        throw new UnsupportedOperationException("\"" + fieldName + "\" is not an array type. It does not support indexed reads.");
    }


    default int hashCode(GenericRecord record, String fieldName) {
        return Objects.hashCode(readGenericRecordOrPrimitive(record, fieldName));
    }

    void writeFieldFromRecordToWriter(DefaultCompactWriter defaultCompactWriter, GenericRecord genericRecord, String fieldName);

    /**
     * @return type size on bytes for compact format which is sometimes different than {@link FieldType#getTypeSize()}
     */
    default int typeSizeInBytes() {
        return VARIABLE_SIZE;
    }
}
