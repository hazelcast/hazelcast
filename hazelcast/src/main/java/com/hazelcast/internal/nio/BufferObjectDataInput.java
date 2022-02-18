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

package com.hazelcast.internal.nio;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.spi.impl.SerializationServiceSupport;

import java.io.IOException;
import java.nio.ByteOrder;

public interface BufferObjectDataInput extends ObjectDataInput, DataReader, SerializationServiceSupport {

    int UTF_BUFFER_SIZE = 1024;

    int read(int position) throws IOException;

    int readInt(int position) throws IOException;

    int readInt(ByteOrder byteOrder) throws IOException;

    int readInt(int position, ByteOrder byteOrder) throws IOException;

    long readLong(int position) throws IOException;

    long readLong(ByteOrder byteOrder) throws IOException;

    long readLong(int position, ByteOrder byteOrder) throws IOException;

    boolean readBoolean(int position) throws IOException;

    byte readByte(int position) throws IOException;

    char readChar(int position) throws IOException;

    double readDouble(int position) throws IOException;

    double readDouble(ByteOrder byteOrder) throws IOException;

    double readDouble(int position, ByteOrder byteOrder) throws IOException;

    float readFloat(int position) throws IOException;

    float readFloat(ByteOrder byteOrder) throws IOException;

    float readFloat(int position, ByteOrder byteOrder) throws IOException;

    short readShort(int position) throws IOException;

    short readShort(ByteOrder byteOrder) throws IOException;

    short readShort(int position, ByteOrder byteOrder) throws IOException;

    int position();

    void position(int newPos);

    void reset();

    void clear();

    void init(byte[] data, int offset);
}
