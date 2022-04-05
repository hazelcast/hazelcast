/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import javax.annotation.Nullable;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Provides serialization methods for arrays by extending DataOutput
 */
public interface ObjectDataOutput extends DataOutput, VersionAware, WanProtocolVersionAware {

    /**
     * @deprecated for the sake of better naming. Use {@link #writeString(String)} instead
     */
    @Deprecated
    void writeUTF(@Nullable String string) throws IOException;

    /**
     * @param string string to be written
     * @throws IOException in case of any exceptional case
     */
    void writeString(@Nullable String string) throws IOException;

    /**
     * @param bytes byte array to be written
     * @throws IOException in case of any exceptional case
     */
    void writeByteArray(@Nullable byte[] bytes) throws IOException;

    /**
     * @param booleans boolean array to be written
     * @throws IOException in case of any exceptional case
     */
    void writeBooleanArray(@Nullable boolean[] booleans) throws IOException;

    /**
     * @param chars char array to be written
     * @throws IOException in case of any exceptional case
     */
    void writeCharArray(@Nullable char[] chars) throws IOException;

    /**
     * @param ints int array to be written
     * @throws IOException in case of any exceptional case
     */
    void writeIntArray(@Nullable int[] ints) throws IOException;

    /**
     * @param longs long array to be written
     * @throws IOException in case of any exceptional case
     */
    void writeLongArray(@Nullable long[] longs) throws IOException;

    /**
     * @param values double array to be written
     * @throws IOException in case of any exceptional case
     */
    void writeDoubleArray(@Nullable double[] values) throws IOException;

    /**
     * @param values float array to be written
     * @throws IOException in case of any exceptional case
     */
    void writeFloatArray(@Nullable float[] values) throws IOException;

    /**
     * @param values short array to be written
     * @throws IOException in case of any exceptional case
     */
    void writeShortArray(@Nullable short[] values) throws IOException;

    /**
     * @param values String array to be written
     * @throws IOException in case of any exceptional case
     * @deprecated for the sake of better naming. Use {@link #writeStringArray(String[])} instead
     */
    @Deprecated
    void writeUTFArray(@Nullable String[] values) throws IOException;

    /**
     * @param values String array to be written
     * @throws IOException in case of any exceptional case
     */
    void writeStringArray(@Nullable String[] values) throws IOException;

    /**
     * @param object object to be written
     * @throws IOException in case of any exceptional case
     */
    void writeObject(@Nullable Object object) throws IOException;

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
}
