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

package com.hazelcast.nio;

import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
* @mdogan 12/31/12
*/
public final class DynamicByteBuffer {

    private ByteBuffer buffer;

    public DynamicByteBuffer(int cap) {
        buffer = ByteBuffer.allocate(cap);
    }

    public DynamicByteBuffer(byte[] array) {
        buffer = ByteBuffer.wrap(array);
    }

    public DynamicByteBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public DynamicByteBuffer compact() {
        buffer.compact();
        return this;
    }

    public DynamicByteBuffer duplicate() {
        return new DynamicByteBuffer(buffer.duplicate());
    }

    public byte get() {
        return buffer.get();
    }

    public byte get(int index) {
        return buffer.get(index);
    }

    public DynamicByteBuffer get(byte[] dst) {
        buffer.get(dst);
        return this;
    }

    public DynamicByteBuffer get(byte[] dst, int offset, int length) {
        buffer.get(dst, offset, length);
        return this;
    }

    public char getChar() {
        return buffer.getChar();
    }

    public char getChar(int index) {
        return buffer.getChar(index);
    }

    public double getDouble() {
        return buffer.getDouble();
    }

    public double getDouble(int index) {
        return buffer.getDouble(index);
    }

    public float getFloat() {
        return buffer.getFloat();
    }

    public float getFloat(int index) {
        return buffer.getFloat(index);
    }

    public int getInt() {
        return buffer.getInt();
    }

    public int getInt(int index) {
        return buffer.getInt(index);
    }

    public long getLong() {
        return buffer.getLong();
    }

    public long getLong(int index) {
        return buffer.getLong(index);
    }

    public short getShort() {
        return buffer.getShort();
    }

    public short getShort(int index) {
        return buffer.getShort(index);
    }

    public DynamicByteBuffer put(byte b) {
        ensureSize(1);
        buffer.put(b);
        return this;
    }

    public DynamicByteBuffer put(int index, byte b) {
        ensureSize(1);
        buffer.put(index, b);
        return this;
    }

    public DynamicByteBuffer put(byte[] src) {
        ensureSize(src.length);
        buffer.put(src);
        return this;
    }

    public DynamicByteBuffer put(byte[] src, int offset, int length) {
        ensureSize(length);
        buffer.put(src, offset, length);
        return this;
    }

    public DynamicByteBuffer put(ByteBuffer src) {
        ensureSize(src.remaining());
        buffer.put(src);
        return this;
    }

    public DynamicByteBuffer putChar(int index, char value) {
        ensureSize(2);
        buffer.putChar(index, value);
        return this;
    }

    public DynamicByteBuffer putChar(char value) {
        ensureSize(2);
        buffer.putChar(value);
        return this;
    }

    public DynamicByteBuffer putDouble(int index, double value) {
        ensureSize(8);
        buffer.putDouble(index, value);
        return this;
    }

    public DynamicByteBuffer putDouble(double value) {
        ensureSize(8);
        buffer.putDouble(value);
        return this;
    }

    public DynamicByteBuffer putFloat(int index, float value) {
        ensureSize(4);
        buffer.putFloat(index, value);
        return this;
    }

    public DynamicByteBuffer putFloat(float value) {
        ensureSize(4);
        buffer.putFloat(value);
        return this;
    }

    public DynamicByteBuffer putInt(int index, int value) {
        ensureSize(4);
        buffer.putInt(index, value);
        return this;
    }

    public DynamicByteBuffer putInt(int value) {
        ensureSize(4);
        buffer.putInt(value);
        return this;
    }

    public DynamicByteBuffer putLong(int index, long value) {
        ensureSize(8);
        buffer.putLong(index, value);
        return this;
    }

    public DynamicByteBuffer putLong(long value) {
        ensureSize(8);
        buffer.putLong(value);
        return this;
    }

    public DynamicByteBuffer putShort(int index, short value) {
        ensureSize(2);
        buffer.putShort(index, value);
        return this;
    }

    public DynamicByteBuffer putShort(short value) {
        ensureSize(2);
        buffer.putShort(value);
        return this;
    }

    public DynamicByteBuffer slice() {
        return new DynamicByteBuffer(buffer.slice());
    }

    private void ensureSize(int i) {
        if (buffer.remaining() < i) {
            int newCap = Math.max(buffer.limit() << 1, buffer.limit() + i);
            ByteBuffer newBuffer = ByteBuffer.allocate(newCap);
            buffer.flip();
            newBuffer.put(buffer);
            buffer = newBuffer;
        }
    }

    public void clear() {
        buffer.clear();
    }

    public void flip() {
        buffer.flip();
    }

    public int limit() {
        return buffer.limit();
    }

    public Buffer limit(int newLimit) {
        return buffer.limit(newLimit);
    }

    public Buffer mark() {
        return buffer.mark();
    }

    public int position() {
        return buffer.position();
    }

    public Buffer position(int newPosition) {
        return buffer.position(newPosition);
    }

    public int remaining() {
        return buffer.remaining();
    }

    public Buffer reset() {
        return buffer.reset();
    }

    public Buffer rewind() {
        return buffer.rewind();
    }

    public int capacity() {
        return buffer.capacity();
    }

    public boolean hasRemaining() {
        return buffer.hasRemaining();
    }

    public void close() {
        buffer = null;
    }
}
