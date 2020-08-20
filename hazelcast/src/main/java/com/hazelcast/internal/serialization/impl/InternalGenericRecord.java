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
 *
 * read*FromArray methods will return `null`
 * 1. if the array is null or empty
 * 2. there is no data at given index. In other words, the index is bigger than the length of the array
 */
public interface InternalGenericRecord extends GenericRecord {

    GenericRecord[] readGenericRecordArray(String fieldName);

    Byte readByteFromArray(String fieldName, int index);

    Boolean readBooleanFromArray(String fieldName, int index);

    Character readCharFromArray(String fieldName, int index);

    Integer readIntFromArray(String fieldName, int index);

    Long readLongFromArray(String fieldName, int index);

    Double readDoubleFromArray(String fieldName, int index);

    Float readFloatFromArray(String fieldName, int index);

    Short readShortFromArray(String fieldName, int index);

    String readUTFFromArray(String fieldName, int index);

    GenericRecord readGenericRecordFromArray(String fieldName, int index);

    Object readObjectFromArray(String fieldName, int index);

    Object[] readObjectArray(String fieldName);

    Object readObject(String fieldName);
}
