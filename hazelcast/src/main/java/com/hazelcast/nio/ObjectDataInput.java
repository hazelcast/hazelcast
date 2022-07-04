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
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Provides serialization methods for arrays of primitive types.
 */
public interface ObjectDataInput extends DataInput, VersionAware, WanProtocolVersionAware {

    /**
     * @deprecated for the sake of better naming. Use {@link #readString()} instead
     */
    @Deprecated
    @Nullable
    String readUTF() throws IOException;

    /**
     * @return the string read
     * @throws IOException if it reaches end of file before finish reading
     */
    @Nullable
    String readString() throws IOException;

    /**
     * @return the byte array read
     * @throws IOException if it reaches end of file before finish reading
     */
    @Nullable
    byte[] readByteArray() throws IOException;

    /**
     * @return the boolean array read
     * @throws IOException if it reaches end of file before finish reading
     */
    @Nullable
    boolean[] readBooleanArray() throws IOException;

    /**
     * @return the char array read
     * @throws IOException if it reaches end of file before finish reading
     */
    @Nullable
    char[] readCharArray() throws IOException;

    /**
     * @return int array read
     * @throws IOException if it reaches end of file before finish reading
     */
    @Nullable
    int[] readIntArray() throws IOException;

    /**
     * @return long array read
     * @throws IOException if it reaches end of file before finish reading
     */
    @Nullable
    long[] readLongArray() throws IOException;

    /**
     * @return double array read
     * @throws IOException if it reaches end of file before finish reading
     */
    @Nullable
    double[] readDoubleArray() throws IOException;

    /**
     * @return float array read
     * @throws IOException if it reaches end of file before finish reading
     */
    @Nullable
    float[] readFloatArray() throws IOException;

    /**
     * @return short array read
     * @throws IOException if it reaches end of file before finish reading
     */
    @Nullable
    short[] readShortArray() throws IOException;

    /**
     * @return String array read
     * @throws IOException if it reaches end of file before finish reading
     * @deprecated for the sake of better naming. Use {@link #readStringArray()} instead
     */
    @Nullable
    @Deprecated
    String[] readUTFArray() throws IOException;

    /**
     * @return String array read
     * @throws IOException if it reaches end of file before finish reading
     */
    @Nullable
    String[] readStringArray() throws IOException;

    /**
     * @param <T> type of the object to be read
     * @return object array read
     * @throws IOException if it reaches end of file before finish reading
     */
    @Nullable
    <T> T readObject() throws IOException;

    /**
     * @param <T>    type of the object to be read
     * @param aClass the type of the class to use when reading
     * @return object array read
     * @throws IOException if it reaches end of file before finish reading
     */
    @Nullable
    <T> T readObject(Class aClass) throws IOException;

    /**
     * Returns class loader that internally used for objects.
     *
     * @return classLoader
     */
    ClassLoader getClassLoader();

    /**
     * @return ByteOrder BIG_ENDIAN or LITTLE_ENDIAN
     */
    ByteOrder getByteOrder();
}
