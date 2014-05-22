/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Provides serialization methods for arrays of primitive types
 */
public interface ObjectDataInput extends DataInput {

    /**
     * @return the char array read
     * @throws IOException if it reaches end of file before finish reading
     */
    char[] readCharArray() throws IOException;

    /**
     * @return int array read
     * @throws IOException if it reaches end of file before finish reading
     */
    int[] readIntArray() throws IOException;

    /**
     * @return long array read
     * @throws IOException if it reaches end of file before finish reading
     */
    long[] readLongArray() throws IOException;

    /**
     * @return double array read
     * @throws IOException if it reaches end of file before finish reading
     */
    double[] readDoubleArray() throws IOException;

    /**
     * @return float array read
     * @throws IOException if it reaches end of file before finish reading
     */
    float[] readFloatArray() throws IOException;

    /**
     * @return short array read
     * @throws IOException if it reaches end of file before finish reading
     */
    short[] readShortArray() throws IOException;

    /**
     * @param <T> type of the object in array to be read
     * @return object array read
     * @throws IOException if it reaches end of file before finish reading
     */
    <T> T readObject() throws IOException;

    /**
     * Returns class loader that internally used for objects
     *
     * @return classLoader
     */
    ClassLoader getClassLoader();

    /**
     * @return ByteOrder BIG_ENDIAN or LITTLE_ENDIAN
     */
    ByteOrder getByteOrder();
}
