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

package com.hazelcast.jet.io.spi.serialization;

import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Provides methods which let to access to serialized data
 * <p>
 * It works like a factory for the memory-blocks creating and returning pointer and sizes
 * It doesn't release memory, memory-releasing is responsibility of the external environment
 */
public interface JetDataOutput extends ObjectDataOutput {

    /**
     * @return address of the last serialized dataBlock
     */
    long getPointer();

    /**
     * @return written bytes into the dataBlock
     */
    long getWrittenSize();

    /**
     * @return real allocated size in bytes of the dataBlock
     */
    long getAllocatedSize();

    void write(long position, int b);

    void writeBoolean(long position, boolean v) throws IOException;

    void writeZeroBytes(int count);

    void writeByte(long position, int v) throws IOException;

    void writeChar(long position, int v) throws IOException;

    void writeDouble(long position, double v) throws IOException;

    void writeDouble(double v, ByteOrder byteOrder) throws IOException;

    void writeDouble(long position, double v, ByteOrder byteOrder) throws IOException;

    void writeFloat(long position, float v) throws IOException;

    void writeFloat(float v, ByteOrder byteOrder) throws IOException;

    void writeFloat(long position, float v, ByteOrder byteOrder) throws IOException;

    void writeInt(long position, int v) throws IOException;

    void writeInt(int v, ByteOrder byteOrder) throws IOException;

    void writeInt(long position, int v, ByteOrder byteOrder) throws IOException;

    void writeLong(long position, long v) throws IOException;

    void writeLong(long v, ByteOrder byteOrder) throws IOException;

    void writeLong(long position, long v, ByteOrder byteOrder) throws IOException;

    void writeShort(long position, int v) throws IOException;

    void writeShort(int v, ByteOrder byteOrder) throws IOException;

    void writeShort(long position, int v, ByteOrder byteOrder) throws IOException;

    long position();

    void position(long newPos);

    void stepOn(long delta);

    void setMemoryManager(MemoryManager memoryManager);

    void clear();
}
