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
import java.util.Set;


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
}
