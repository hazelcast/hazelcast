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

package com.hazelcast.nio.serialization;

import com.hazelcast.nio.ObjectDataInput;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author mdogan 12/28/12
 */
public interface PortableReader {

    int getVersion();

    boolean hasField(String fieldName);

    Set<String> getFieldNames();

    FieldType getFieldType(String fieldName);

    int getFieldClassId(String fieldName);

    int readInt(String fieldName) throws IOException;

    long readLong(String fieldName) throws IOException;

    String readUTF(String fieldName) throws IOException;

    boolean readBoolean(String fieldName) throws IOException;

    byte readByte(String fieldName) throws IOException;

    char readChar(String fieldName) throws IOException;

    double readDouble(String fieldName) throws IOException;

    float readFloat(String fieldName) throws IOException;

    short readShort(String fieldName) throws IOException;
    
    <P extends Portable> P readPortable(String fieldName) throws IOException;

    byte[] readByteArray(String fieldName) throws IOException;

    char[] readCharArray(String fieldName) throws IOException;

    int[] readIntArray(String fieldName) throws IOException;

    long[] readLongArray(String fieldName) throws IOException;

    double[] readDoubleArray(String fieldName) throws IOException;

    float[] readFloatArray(String fieldName) throws IOException;

    short[] readShortArray(String fieldName) throws IOException;

    Portable[] readPortableArray(String fieldName) throws IOException;

    ObjectDataInput getRawDataInput() throws IOException;

    /**
     * Reads an object from a field.
     *
     * @param fieldName the name of the field
     * @return the read object
     * @throws IOException if serialization fails.
     */
    <T> T readObject(String fieldName) throws IOException;

    /**
     * Reads an object array from a field.
     *
     * @param fieldName the name of the field
     * @return the read object array
     * @throws IOException if serialization fails.
     */
    <T> T[] readObjectArray(String fieldName,  final Class<T[]> clazz) throws IOException;

    /**
     * Reads a map from a field into the provided map.
     *
     * @param fieldName the name of the field
     * @param map the map to populate
     * @throws IOException if serialization fails.
     */
    <K, V> void readMap(String fieldName, Map<K, V> map) throws IOException;

    /**
     * Reads a collection from a field into the provided collection.
     *
     * @param fieldName the name of the field
     * @param collection the collection to populate
     * @throws IOException if serialization fails.
     */
    <T> void readCollection(String fieldName, Collection<T> collection) throws IOException;


}
