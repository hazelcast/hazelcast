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

import java.io.IOException;

/**
 * Additionally to GenericRecord, this one has more methods to be used in Query.
 *
 * @see GenericRecordQueryReader
 * @see CachedGenericRecordQueryReader
 */
public interface InternalGenericRecord extends GenericRecord{

    GenericRecord[] readGenericRecordArray(String fieldName) throws IOException;

    Byte readByteFromArray(String fieldName, int index) throws IOException;

    Boolean readBooleanFromArray(String fieldName, int index) throws IOException;

    Character readCharFromArray(String fieldName, int index) throws IOException;

    Integer readIntFromArray(String fieldName, int index) throws IOException;

    Long readLongFromArray(String fieldName, int index) throws IOException;

    Double readDoubleFromArray(String fieldName, int index) throws IOException;

    Float readFloatFromArray(String fieldName, int index) throws IOException;

    Short readShortFromArray(String fieldName, int index) throws IOException;

    String readUTFFromArray(String fieldName, int index) throws IOException;

    GenericRecord readGenericRecordFromArray(String fieldName, int index) throws IOException;

    Object readObjectFromArray(String fieldName, int index) throws IOException;

    Object[] readObjectArray(String fieldName) throws IOException;

    Object readObject(String fieldName) throws IOException;
}
