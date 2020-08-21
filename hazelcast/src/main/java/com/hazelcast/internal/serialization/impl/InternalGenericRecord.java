/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.GenericRecord;

/**
 * Additionally to GenericRecord, this one has more methods to be used in Query.
 *
 * @see GenericRecordQueryReader
 * <p>
 * read*FromArray methods will return `null`
 * 1. if the array is null or empty
 * 2. there is no data at given index. In other words, the index is bigger than the length of the array
 */
public interface InternalGenericRecord extends GenericRecord {

    Boolean readBooleanFromArray(String fieldName, int index);

    Byte readByteFromArray(String fieldName, int index);

    Character readCharFromArray(String fieldName, int index);

    Double readDoubleFromArray(String fieldName, int index);

    Float readFloatFromArray(String fieldName, int index);

    Integer readIntFromArray(String fieldName, int index);

    Long readLongFromArray(String fieldName, int index);

    Short readShortFromArray(String fieldName, int index);

    String readUTFFromArray(String fieldName, int index);

    GenericRecord readGenericRecordFromArray(String fieldName, int index);

    /**
     * Reads same value {@link InternalGenericRecord#readGenericRecord(String)} }, but in deserialized form.
     * This is used in query system when the object is leaf of the query.
     *
     * @param fieldName
     * @param index     array index to read from
     * @return a nested field as a concrete deserialized object rather than generic record
     */
    Object readObjectFromArray(String fieldName, int index);

    /**
     * Reads same value {@link GenericRecord#readGenericRecordArray(String)}, but in deserialized form.
     * This is used in query system when the object is leaf of the query.
     *
     * @param fieldName
     * @return a nested field as array of deserialized objects rather than array of the  generic records
     */
    Object[] readObjectArray(String fieldName);

    /**
     * Reads same value {@link GenericRecord#readGenericRecord(String)} }, but in deserialized form.
     * This is used in query system when the object is leaf of the query.
     *
     * @param fieldName
     * @return a nested field as a concrete deserialized object rather than generic record
     */
    Object readObject(String fieldName);
}
