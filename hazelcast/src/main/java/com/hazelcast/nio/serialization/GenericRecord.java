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

package com.hazelcast.nio.serialization;

/**
 * A generic object interface that is returned when the domain class can not be created.
 * Currently this is valid for {@link Portable} objects.
 */
public interface GenericRecord {

    /**
     * @return an empty generic record builder with same class definition as this one
     */
    GenericRecordBuilder createGenericRecordBuilder();

    /**
     * Returned GenericRecordBuilder can be used to have exact copy and also just to update a couple of fields. By default,
     * it will copy all the fields.
     *
     * @return a generic record builder with same class definition as this one and populated with same values.
     */
    GenericRecordBuilder cloneWithGenericRecordBuilder();

    FieldType getFieldType(String fieldName);

    GenericRecord[] readGenericRecordArray(String fieldName);

    GenericRecord readGenericRecord(String fieldName);

    boolean hasField(String fieldName);

    int readInt(String fieldName);

    long readLong(String fieldName);

    String readUTF(String fieldName);

    boolean readBoolean(String fieldName);

    byte readByte(String fieldName);

    char readChar(String fieldName);

    double readDouble(String fieldName);

    float readFloat(String fieldName);

    short readShort(String fieldName);

    byte[] readByteArray(String fieldName);

    boolean[] readBooleanArray(String fieldName);

    char[] readCharArray(String fieldName);

    int[] readIntArray(String fieldName);

    long[] readLongArray(String fieldName);

    double[] readDoubleArray(String fieldName);

    float[] readFloatArray(String fieldName);

    short[] readShortArray(String fieldName);

    String[] readUTFArray(String fieldName);

}
