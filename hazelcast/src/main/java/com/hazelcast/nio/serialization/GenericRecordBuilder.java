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

import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecordBuilder;

/**
 * Interface for creating GenericRecords
 * Allows using Portable without having the domain class
 */
public interface GenericRecordBuilder {

    /**
     * @param classDefinition of the portable that we will create
     * @return GenericRecordBuilder for Portable format
     */
    static GenericRecordBuilder portable(ClassDefinition classDefinition) {
        return new PortableGenericRecordBuilder(classDefinition);
    }

    GenericRecord build();

    GenericRecordBuilder writeInt(String fieldName, int value);

    GenericRecordBuilder writeLong(String fieldName, long value);

    GenericRecordBuilder writeUTF(String fieldName, String value);

    GenericRecordBuilder writeBoolean(String fieldName, boolean value);

    GenericRecordBuilder writeByte(String fieldName, byte value);

    GenericRecordBuilder writeChar(String fieldName, char value);

    GenericRecordBuilder writeDouble(String fieldName, double value);

    GenericRecordBuilder writeFloat(String fieldName, float value);

    GenericRecordBuilder writeShort(String fieldName, short value);

    GenericRecordBuilder writeGenericRecord(String fieldName, GenericRecord value);

    GenericRecordBuilder writeGenericRecordArray(String fieldName, GenericRecord[] value);

    GenericRecordBuilder writeByteArray(String fieldName, byte[] value);

    GenericRecordBuilder writeBooleanArray(String fieldName, boolean[] value);

    GenericRecordBuilder writeCharArray(String fieldName, char[] value);

    GenericRecordBuilder writeIntArray(String fieldName, int[] value);

    GenericRecordBuilder writeLongArray(String fieldName, long[] value);

    GenericRecordBuilder writeDoubleArray(String fieldName, double[] value);

    GenericRecordBuilder writeFloatArray(String fieldName, float[] value);

    GenericRecordBuilder writeShortArray(String fieldName, short[] value);

    GenericRecordBuilder writeUTFArray(String fieldName, String[] value);
}
