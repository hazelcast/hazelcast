/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.nio.Bits;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Implementation of ClientProtocolBuffer that is used by default in clients.
 * There is another unsafe implementation which is configurable .
 * See  {@link UnsafeBuffer}.
 */
public class SafeBuffer implements ClientProtocolBuffer {

    private ByteBuffer byteBuffer;

    public SafeBuffer(byte[] buffer) {
        wrap(buffer);
    }

    @Override
    public void putLong(int index, long value) {
        byteBuffer.putLong(index, value);
    }

    @Override
    public void putInt(int index, int value) {
        byteBuffer.putInt(index, value);
    }

    @Override
    public void putShort(int index, short value) {
        byteBuffer.putShort(index, value);
    }

    @Override
    public void putByte(int index, byte value) {
        byteBuffer.put(index, value);
    }

    @Override
    public void putBytes(int index, byte[] src) {
        putBytes(index, src, 0, src.length);
    }

    @Override
    public void putBytes(int index, byte[] src, int offset, int length) {
        byteBuffer.position(index);
        byteBuffer.put(src, offset, length);
    }

    @Override
    public int putStringUtf8(int index, String value) {
        return putStringUtf8(index, value, Integer.MAX_VALUE);
    }

    @Override
    public int putStringUtf8(int index, String value, int maxEncodedSize) {
        final byte[] bytes = value.getBytes(Bits.UTF_8);
        if (bytes.length > maxEncodedSize) {
            throw new IllegalArgumentException("Encoded string larger than maximum size: " + maxEncodedSize);
        }

        putInt(index, bytes.length);
        putBytes(index + Bits.INT_SIZE_IN_BYTES, bytes);

        return Bits.INT_SIZE_IN_BYTES + bytes.length;
    }

    @Override
    public void wrap(byte[] buffer) {
        byteBuffer = ByteBuffer.wrap(buffer);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public byte[] byteArray() {
        return byteBuffer.array();
    }

    @Override
    public int capacity() {
        return byteBuffer.capacity();
    }

    @Override
    public long getLong(int index) {

        return byteBuffer.getLong(index);
    }

    @Override
    public int getInt(int index) {
        return byteBuffer.getInt(index);
    }

    @Override
    public short getShort(int index) {
        return byteBuffer.getShort(index);
    }

    @Override
    public byte getByte(int index) {
        return byteBuffer.get(index);
    }

    @Override
    public void getBytes(int index, byte[] dst) {
        getBytes(index, dst, 0, dst.length);
    }

    @Override
    public void getBytes(int index, byte[] dst, int offset, int length) {
        byteBuffer.position(index);
        byteBuffer.get(dst, offset, length);
    }

    @Override
    public String getStringUtf8(int offset, int length) {
        final byte[] stringInBytes = new byte[length];
        getBytes(offset + Bits.INT_SIZE_IN_BYTES, stringInBytes);

        return new String(stringInBytes, Bits.UTF_8);
    }

}
