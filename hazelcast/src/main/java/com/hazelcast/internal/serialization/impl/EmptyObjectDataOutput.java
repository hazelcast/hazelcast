/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.nio.ByteOrder;

@SuppressWarnings("checkstyle:methodcount")
final class EmptyObjectDataOutput extends VersionedObjectDataOutput implements ObjectDataOutput {

    @Override
    public void writeObject(Object object) {
    }

    @Override
    public void writeData(Data data) {
    }

    @Override
    public void write(int b) {
    }

    @Override
    public void write(byte[] b) {
    }

    @Override
    public void write(byte[] b, int off, int len) {
    }

    @Override
    public void writeBoolean(boolean v) {
    }

    @Override
    public void writeByte(int v) {
    }

    @Override
    public void writeShort(int v) {
    }

    @Override
    public void writeChar(int v) {
    }

    @Override
    public void writeInt(int v) {
    }

    @Override
    public void writeLong(long v) {
    }

    @Override
    public void writeFloat(float v) {
    }

    @Override
    public void writeDouble(double v) {
    }

    @Override
    public void writeBytes(String s) {
    }

    @Override
    public void writeChars(String s) {
    }

    @Override
    public void writeUTF(String s) {
    }

    @Override
    public void writeByteArray(byte[] value) {
    }

    @Override
    public void writeBooleanArray(boolean[] booleans) {
    }

    @Override
    public void writeCharArray(char[] chars) {
    }

    @Override
    public void writeIntArray(int[] ints) {
    }

    @Override
    public void writeLongArray(long[] longs) {
    }

    @Override
    public void writeDoubleArray(double[] values) {
    }

    @Override
    public void writeFloatArray(float[] values) {
    }

    @Override
    public void writeShortArray(short[] values) {
    }

    @Override
    public void writeUTFArray(String[] values) {
    }

    @Override
    public byte[] toByteArray() {
        return toByteArray(0);
    }

    @Override
    public byte[] toByteArray(int padding) {
        throw new UnsupportedOperationException();
    }

    public void close() {
    }

    @Override
    public ByteOrder getByteOrder() {
        return ByteOrder.BIG_ENDIAN;
    }

}
