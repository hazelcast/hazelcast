/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Provides serialization methods for arrays by extending DataOutput
 */
public interface ObjectDataOutput extends DataOutput, VersionAware, WanProtocolVersionAware {

    /**
     * @param bytes byte array to be written
     * @throws IOException
     */
    void writeByteArray(byte[] bytes) throws IOException;

    /**
     * @param booleans boolean array to be written
     * @throws IOException
     */
    void writeBooleanArray(boolean[] booleans) throws IOException;

    /**
     * @param chars char array to be written
     * @throws IOException
     */
    void writeCharArray(char[] chars) throws IOException;

    /**
     * @param ints int array to be written
     * @throws IOException
     */
    void writeIntArray(int[] ints) throws IOException;

    /**
     * @param longs long array to be written
     * @throws IOException
     */
    void writeLongArray(long[] longs) throws IOException;

    /**
     * @param values double array to be written
     * @throws IOException
     */
    void writeDoubleArray(double[] values) throws IOException;

    /**
     * @param values float array to be written
     * @throws IOException
     */
    void writeFloatArray(float[] values) throws IOException;

    /**
     * @param values short array to be written
     * @throws IOException
     */
    void writeShortArray(short[] values) throws IOException;

    /**
     * @param values String array to be written
     * @throws IOException
     */
    void writeUTFArray(String[] values) throws IOException;

    /**
     * @param object object to be written
     * @throws IOException
     */
    void writeObject(Object object) throws IOException;

    /**
     * @param data data to be written
     * @throws IOException
     */
    void writeData(Data data) throws IOException;

    /**
     * @return copy of internal byte array
     */
    byte[] toByteArray();

    /**
     * @param padding padding bytes at the beginning of the byte-array.
     * @return copy of internal byte array
     */
    byte[] toByteArray(int padding);

    /**
     * @return ByteOrder BIG_ENDIAN or LITTLE_ENDIAN
     */
    ByteOrder getByteOrder();

    /**
     * @return serialization service for this object
     */
    SerializationService getSerializationService();
}
