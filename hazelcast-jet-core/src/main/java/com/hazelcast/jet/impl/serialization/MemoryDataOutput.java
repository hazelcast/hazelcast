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
import java.io.DataOutput;

public class MemoryDataOutput implements DataOutput {

    private static final MemoryWriter WRITER = MemoryWriter.create();

    private byte[] buffer;
    private int position;

    public MemoryDataOutput(int size) {
        this.buffer = new byte[size];
        this.position = 0;
    }

    @Override
    public void write(int b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(@Nonnull byte[] b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(@Nonnull byte[] b, int off, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBoolean(boolean v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeByte(int v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeShort(int v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeChar(int v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeInt(int v) {
        ensureAvailable(Integer.BYTES);
        WRITER.writeInt(buffer, position, v);
        position += Integer.BYTES;
    }

    @Override
    public void writeLong(long v) {
        ensureAvailable(Long.BYTES);
        WRITER.writeLong(buffer, position, v);
        position += Long.BYTES;
    }

    @Override
    public void writeFloat(float v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeDouble(double v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBytes(@Nonnull String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeChars(@Nonnull String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeUTF(@Nonnull String s) {
        throw new UnsupportedOperationException();
    }

    private int available() {
        return buffer.length - position;
    }

    private void ensureAvailable(int length) {
        if (length > available()) {
            int capacity = Math.max(buffer.length << 1, buffer.length + length);
            byte[] buffer = new byte[capacity];
            System.arraycopy(this.buffer, 0, buffer, 0, position);
            this.buffer = buffer;
        }
    }

    public byte[] toByteArray() {
        byte[] buffer = new byte[position];
        System.arraycopy(this.buffer, 0, buffer, 0, position);
        return buffer;
    }
}
