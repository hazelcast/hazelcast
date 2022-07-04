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

import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.SerializationServiceSupport;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteOrder;

public interface BufferObjectDataOutput extends ObjectDataOutput, Closeable,
                                                SerializationServiceSupport, DataWriter {

    void write(int position, int b);

    void writeInt(int position, int v) throws IOException;

    void writeInt(int v, ByteOrder byteOrder) throws IOException;

    void writeInt(int position, int v, ByteOrder byteOrder) throws IOException;

    void writeLong(int position, long v) throws IOException;

    void writeLong(long v, ByteOrder byteOrder) throws IOException;

    void writeLong(int position, long v, ByteOrder byteOrder) throws IOException;

    void writeBoolean(int position, boolean v) throws IOException;

    void writeBooleanBit(int position, int bitIndex, boolean v) throws IOException;

    void writeByte(int position, int v) throws IOException;

    void writeZeroBytes(int count);

    void writeChar(int position, int v) throws IOException;

    void writeDouble(int position, double v) throws IOException;

    void writeDouble(double v, ByteOrder byteOrder) throws IOException;

    void writeDouble(int position, double v, ByteOrder byteOrder) throws IOException;

    void writeFloat(int position, float v) throws IOException;

    void writeFloat(float v, ByteOrder byteOrder) throws IOException;

    void writeFloat(int position, float v, ByteOrder byteOrder) throws IOException;

    void writeShort(int position, int v) throws IOException;

    void writeShort(int v, ByteOrder byteOrder) throws IOException;

    void writeShort(int position, int v, ByteOrder byteOrder) throws IOException;

    int position();

    void position(int newPos);

    void clear();
}
