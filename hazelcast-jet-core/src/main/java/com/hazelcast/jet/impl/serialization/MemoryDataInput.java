/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.serialization;

import javax.annotation.Nonnull;
import java.io.DataInput;

public class MemoryDataInput implements DataInput {

    private static final MemoryReader READER = MemoryReader.create();

    private final byte[] buffer;
    private int position;

    public MemoryDataInput(byte[] buffer) {
        this.buffer = buffer;
        this.position = 0;
    }

    @Override
    public void readFully(@Nonnull byte[] b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(@Nonnull byte[] b, int off, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int skipBytes(int n) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean readBoolean() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte readByte() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readUnsignedByte() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short readShort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readUnsignedShort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public char readChar() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readInt() {
        checkAvailable(Integer.BYTES);
        int value = READER.readInt(buffer, position);
        position += Integer.BYTES;
        return value;
    }

    @Override
    public long readLong() {
        checkAvailable(Long.BYTES);
        long value = READER.readLong(buffer, position);
        position += Long.BYTES;
        return value;
    }

    @Override
    public float readFloat() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double readDouble() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readLine() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public String readUTF() {
        throw new UnsupportedOperationException();
    }

    private void checkAvailable(int length) {
        if (position + length > buffer.length) {
            throw new RuntimeException("Cannot read " + length + " bytes");
        }
    }
}
