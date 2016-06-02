/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.io.api.serialization;

import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.nio.ObjectDataInput;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Provides serialization methods for arrays of primitive types which are located in off-heap
 * Let us to work with long pointers
 */
public interface JetDataInput extends ObjectDataInput {

    int read() throws IOException;

    int read(long position) throws IOException;

    boolean readBoolean(long position) throws IOException;

    byte readByte(long position) throws IOException;

    char readChar(long position) throws IOException;

    double readDouble(int position) throws IOException;

    double readDouble(ByteOrder byteOrder) throws IOException;

    double readDouble(long position, ByteOrder byteOrder) throws IOException;

    float readFloat(int position) throws IOException;

    float readFloat(ByteOrder byteOrder) throws IOException;

    float readFloat(int position, ByteOrder byteOrder) throws IOException;

    int readInt(ByteOrder byteOrder) throws IOException;

    int readInt(long position, ByteOrder byteOrder) throws IOException;

    long readLong(ByteOrder byteOrder) throws IOException;

    long readLong(long position, ByteOrder byteOrder) throws IOException;

    short readShort(long position) throws IOException;

    short readShort(ByteOrder byteOrder) throws IOException;

    short readShort(long position, ByteOrder byteOrder) throws IOException;

    long position();

    void position(long newPos);

    long available();

    void reset(long pointer, long size);

    void setMemoryManager(MemoryManager memoryManager);

    void clear();

    void close();
}
