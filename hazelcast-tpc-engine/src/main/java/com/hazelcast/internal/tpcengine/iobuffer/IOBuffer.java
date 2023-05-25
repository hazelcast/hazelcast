/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.iobuffer;

import com.hazelcast.internal.tpcengine.net.AsyncSocket;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_BYTE;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_CHAR;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_INT;
import static com.hazelcast.internal.tpcengine.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpcengine.util.BitUtil.nextPowerOfTwo;


/**
 * The IOBuffer is used to read/write bytes from I/O devices like a socket or a file.
 * <p>
 * Currently, the IOBuffer has one ByteBuffer underneath. The problem is that if you have very large
 * payloads, a single chunk of memory is needed for those bytes. This can lead to allocation problems
 * due to fragmentation (perhaps it can't be allocated because of fragmentation), but it can also
 * lead to fragmentation because buffers can have different sizes.
 * <p>
 * So instead of having a single ByteBuffer underneath, allow for a list of ByteBuffer all with some
 * fixed size, e.g. up to 16 KB. So if a 1MB chunk of data is received, just 64 byte-arrays of 16KB.
 * This will prevent the above fragmentation problem although it could lead to some increased
 * internal fragmentation because more memory is allocated than used. I believe this isn't such a
 * big problem because IOBuffer are short lived.
 * <p>
 * So if an IOBuffer should contain a list of ByteBuffers, then regular reading/writing to the IOBuffer
 * should be agnostic of the composition.
 * <p>
 * Another feature that is required is ability to block align the pointer. This is needed for O_DIRECT. Probably
 * this isn't needed when we can just pass any pointer. It should be the task of the pointer provider to block align.
 * <p>
 * Also the ability to wrap any pointer. For more information see:
 * https://stackoverflow.com/questions/16465477/is-there-a-way-to-create-a-direct-bytebuffer-from-a-pointer-solely-in-java
 * E.g. in case of the buffer pool (application specific page cache) we just want to take a pointer to
 * some memory in the bufferpool and pass it to the IOBuffer for reading/writing that page to disk.
 */
@SuppressWarnings({"checkstyle:VisibilityModifier", "checkstyle:MethodCount", "java:S1149", "java:S1135"})
public class IOBuffer {

    IOBuffer next;
    AsyncSocket socket;

    boolean trackRelease;
    IOBufferAllocator allocator;
    boolean concurrent;

    // make field?
    AtomicInteger refCount = new AtomicInteger();

    private ByteBuffer buff;

    public IOBuffer(int size) {
        this(size, false);
    }

    public IOBuffer(int size, boolean direct) {
        //todo: allocate power of 2.
        this.buff = direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    public IOBuffer(ByteBuffer buffer) {
        this.buff = buffer;
    }

    public ByteBuffer byteBuffer() {
        return buff;
    }

    public void clear() {
        buff.clear();
    }

    public int position() {
        return buff.position();
    }

    public void position(int position) {
        buff.position(position);
    }

    public void incPosition(int delta) {
        buff.position(buff.position() + delta);
    }

    public void flip() {
        buff.flip();
    }

    /**
     * Returns the number of bytes remaining in this buffer for reading or writing
     *
     * @return
     */
    public int remaining() {
        return buff.remaining();
    }

    public void ensureRemaining(int remaining) {
        if (buff.remaining() < remaining) {
            int newCapacity = nextPowerOfTwo(buff.capacity() + remaining);

            ByteBuffer newBuffer = buff.hasArray()
                    ? ByteBuffer.allocate(newCapacity)
                    : ByteBuffer.allocateDirect(newCapacity);
            buff.flip();
            newBuffer.put(buff);
            buff = newBuffer;
        }
    }

    public byte getByte(int index) {
        return buff.get(index);
    }

    public void writeByte(byte value) {
        ensureRemaining(SIZEOF_BYTE);
        buff.put(value);
    }

    public void writeSizedBytes(byte[] src) {
        ensureRemaining(SIZEOF_INT + src.length);
        buff.putInt(src.length);
        buff.put(src);
    }

    public void writeBytes(byte[] src) {
        ensureRemaining(src.length);
        buff.put(src);
    }

    public void readBytes(byte[] dst, int len) {
        buff.get(dst, 0, len);
    }

    public char readChar() {
        return buff.getChar();
    }

    public void writeChar(char value) {
        ensureRemaining(SIZEOF_CHAR);
        buff.putChar(value);
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    public void writeShortL(short v) {
        ensureRemaining(SIZEOF_INT);
        buff.put((byte) ((v) & 0xFF));
        buff.put((byte) ((v >>> 8) & 0xFF));
    }

    public int getInt(int index) {
        return buff.getInt(index);
    }

    public int readInt() {
        return buff.getInt();
    }

    public void putInt(int index, int value) {
        buff.putInt(index, value);
    }

    public void writeInt(int value) {
        ensureRemaining(SIZEOF_INT);
        buff.putInt(value);
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    public void writeIntL(int v) {
        ensureRemaining(SIZEOF_INT);
        buff.put((byte) ((v) & 0xFF));
        buff.put((byte) ((v >>> 8) & 0xFF));
        buff.put((byte) ((v >>> 16) & 0xFF));
        buff.put((byte) ((v >>> 24) & 0xFF));
    }

    public long readLong() {
        return buff.getLong();
    }

    public long getLong(int index) {
        return buff.getLong(index);
    }

    public void putLong(int index, long value) {
        buff.putLong(index, value);
    }

    public void writeLong(long value) {
        ensureRemaining(SIZEOF_LONG);
        buff.putLong(value);
    }

    public void write(ByteBuffer src) {
        write(src, src.remaining());
    }

    public void write(ByteBuffer src, int count) {
        ensureRemaining(count);

        if (src.remaining() <= count) {
            buff.put(src);
        } else {
            int limit = src.limit();
            src.limit(src.position() + count);
            buff.put(src);
            src.limit(limit);
        }
    }

    // very inefficient
    public void writeString(String s) {
        int length = s.length();

        ensureRemaining(SIZEOF_INT + length * SIZEOF_CHAR);

        buff.putInt(length);
        for (int k = 0; k < length; k++) {
            buff.putChar(s.charAt(k));
        }
    }

    // very inefficient
    public void readString(StringBuffer sb) {
        int size = buff.getInt();
        for (int k = 0; k < size; k++) {
            sb.append(buff.getChar());
        }
    }

    public String readString() {
        StringBuilder sb = new StringBuilder();
        int size = buff.getInt();
        for (int k = 0; k < size; k++) {
            sb.append(buff.getChar());
        }
        return sb.toString();
    }

    public void acquire() {
        if (allocator == null) {
            return;
        }

        if (concurrent) {
            for (; ; ) {
                int current = refCount.get();
                if (current == 0) {
                    throw new IllegalStateException("Can't acquire a freed IOBuffer");
                }

                if (refCount.compareAndSet(current, current + 1)) {
                    break;
                }
            }
        } else {
            refCount.lazySet(refCount.get() + 1);
        }
    }

    public int refCount() {
        return refCount.get();
    }

    @SuppressWarnings({"checkstyle:NestedIfDepth", "java:S3776"})
    public void release() {
        if (allocator == null) {
            return;
        }

        if (concurrent) {
            for (; ; ) {
                int current = refCount.get();
                if (current > 0) {
                    if (refCount.compareAndSet(current, current - 1)) {
                        if (current == 1) {
                            allocator.free(this);
                        }
                        break;
                    }
                } else {
                    throw new IllegalStateException("Too many releases. Ref counter must be larger than 0, current:" + current);
                }
            }
        } else {
            int current = refCount.get();
            if (current == 1) {
                refCount.lazySet(0);
                allocator.free(this);
            } else if (current > 1) {
                refCount.lazySet(current - 1);
            } else {
                throw new IllegalStateException("Too many releases. Ref counter must be larger than 0, current:" + current);
            }
        }
    }
}
