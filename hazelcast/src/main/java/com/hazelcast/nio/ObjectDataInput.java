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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Provides serialization methods for arrays of primitive types.
 */
public interface ObjectDataInput extends DataInput, VersionAware, WanProtocolVersionAware {

    /**
     * @return the byte array read
     * @throws IOException if it reaches end of file before finish reading
     */
    byte[] readByteArray() throws IOException;

    /**
     * @return the boolean array read
     * @throws IOException if it reaches end of file before finish reading
     */
    boolean[] readBooleanArray() throws IOException;

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
     * @return String array read
     * @throws IOException if it reaches end of file before finish reading
     */
    String[] readUTFArray() throws IOException;

    /**
     * @param <T> type of the object to be read
     * @return object array read
     * @throws IOException if it reaches end of file before finish reading
     */
    <T> T readObject() throws IOException;

    /**
     * Reads to stored Data as an object instead of a Data instance.
     * <p>
     * The reason this method exists is that in some cases {@link Data} is stored on serialization, but on deserialization
     * the actual object instance is needed. Getting access to the {@link Data} is easy by calling the {@link #readData()}
     * method. But de-serializing the {@link Data} to an object instance is impossible because there is no reference to the
     * {@link SerializationService}.
     *
     * @param <T> type of the object to be read
     * @return the read Object
     * @throws IOException if it reaches end of file before finish reading
     */
    <T> T readDataAsObject() throws IOException;

    /**
     * @param <T>    type of the object to be read
     * @param aClass the type of the class to use when reading
     * @return object array read
     * @throws IOException if it reaches end of file before finish reading
     */
    <T> T readObject(Class aClass) throws IOException;

    /**
     * @return data read
     * @throws IOException if it reaches end of file before finish reading
     */
    Data readData() throws IOException;

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

    /**
     * @return serialization service for this object
     */
    InternalSerializationService getSerializationService();
}
