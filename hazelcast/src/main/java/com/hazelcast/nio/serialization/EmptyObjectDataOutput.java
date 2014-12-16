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

import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.nio.ByteOrder;

final class EmptyObjectDataOutput implements ObjectDataOutput {

    public void writeObject(Object object) throws IOException {
    }

    public void writeData(Data data) throws IOException {
    }

    public void write(int b) throws IOException {
    }

    public void write(byte[] b) throws IOException {
    }

    public void write(byte[] b, int off, int len) throws IOException {
    }

    public void writeBoolean(boolean v) throws IOException {
    }

    public void writeByte(int v) throws IOException {
    }

    public void writeShort(int v) throws IOException {
    }

    public void writeChar(int v) throws IOException {
    }

    public void writeInt(int v) throws IOException {
    }

    public void writeLong(long v) throws IOException {
    }

    public void writeFloat(float v) throws IOException {
    }

    public void writeDouble(double v) throws IOException {
    }

    public void writeBytes(String s) throws IOException {
    }

    public void writeChars(String s) throws IOException {
    }

    public void writeUTF(String s) throws IOException {
    }

    public void writeByteArray(byte[] value) throws IOException {
    }

    public void writeCharArray(char[] chars) throws IOException {
    }

    public void writeIntArray(int[] ints) throws IOException {
    }

    public void writeLongArray(long[] longs) throws IOException {
    }

    public void writeDoubleArray(double[] values) throws IOException {
    }

    public void writeFloatArray(float[] values) throws IOException {
    }

    public void writeShortArray(short[] values) throws IOException {
    }

    public byte[] toByteArray() {
        throw new UnsupportedOperationException();
    }

    public void close() throws IOException {
    }

    public ByteOrder getByteOrder() {
        return ByteOrder.BIG_ENDIAN;
    }
}
