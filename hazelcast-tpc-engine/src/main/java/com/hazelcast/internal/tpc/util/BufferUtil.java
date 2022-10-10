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

package com.hazelcast.internal.tpc.util;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

public final class BufferUtil {

    private static final Field ADDRESS;
    private static final Field CAPACITY;
    private static final long ADDRESS_OFFSET;
    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;
    private static final int PAGE_SIZE = OS.pageSize();
    private static final long CAPACITY_OFFSET;

    static {
        try {
            ADDRESS = Buffer.class.getDeclaredField("address");
            CAPACITY = Buffer.class.getDeclaredField("capacity");
            ADDRESS_OFFSET = UNSAFE.objectFieldOffset(ADDRESS);
            CAPACITY_OFFSET = UNSAFE.objectFieldOffset(CAPACITY);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static ByteBuffer allocateDirectPageAligned(int capacity) {
        return allocateDirectAligned(capacity, PAGE_SIZE);
    }

    public static ByteBuffer allocateDirectAligned(int capacity, int alignment) {
        if (alignment < 0) {
            throw new IllegalArgumentException("alignment can't be smaller than 1");
        } else if (alignment == 1) {
            return ByteBuffer.allocateDirect(capacity);
        } else {
            long rawAddress = UNSAFE.allocateMemory(capacity + alignment);
            long address = toAlignedAddress(rawAddress, alignment);
            return allocateDirect(address, capacity);
        }
    }

    public static ByteBuffer allocateDirect(long address, int capacity) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(0);
        UNSAFE.putLong(buffer, ADDRESS_OFFSET, address);
        UNSAFE.putInt(buffer, CAPACITY_OFFSET, capacity);
        buffer.limit(capacity);
        return buffer;
    }

    public static long addressOf(ByteBuffer byteBuffer) {
        if(!byteBuffer.isDirect()){
            throw new IllegalArgumentException("Only direct bytebuffers allowed");
        }
        return UNSAFE.getLong(byteBuffer, ADDRESS_OFFSET);
    }

    public static long toPageAlignedAddress(long rawAddress) {
        if (rawAddress % PAGE_SIZE == 0) {
            return rawAddress;
        } else {
            return rawAddress - rawAddress % PAGE_SIZE + PAGE_SIZE;
        }
    }

    public static long toAlignedAddress(long rawAddress, int alignment) {
        if (rawAddress % alignment == 0) {
            return rawAddress;
        } else {
            return rawAddress - rawAddress % alignment + alignment;
        }
    }

    private BufferUtil() {
    }

    /**
     * Creates a debug String for te given ByteBuffer. Useful when debugging IO.
     * <p>
     * Do not remove even if this method isn't used.
     *
     * @param name       name of the ByteBuffer.
     * @param byteBuffer the ByteBuffer
     * @return the debug String
     */
    public static String toDebugString(String name, ByteBuffer byteBuffer) {
        return name + "(pos:" + byteBuffer.position() + " lim:" + byteBuffer.limit()
                + " remain:" + byteBuffer.remaining() + " cap:" + byteBuffer.capacity() + ")";
    }

    /**
     * Compacts or clears the buffer depending if bytes are remaining in the byte-buffer.
     *
     * @param bb the ByteBuffer
     */
    public static void compactOrClear(ByteBuffer bb) {
        if (bb.hasRemaining()) {
            bb.compact();
        } else {
            upcast(bb).clear();
        }
    }

    /**
     * Explicit cast to {@link Buffer} parent buffer type. It resolves issues with covariant return types in Java 9+ for
     * {@link ByteBuffer} and {@link java.nio.CharBuffer}. Explicit casting resolves the NoSuchMethodErrors (e.g
     * java.lang.NoSuchMethodError: java.nio.ByteBuffer.limit(I)Ljava/nio/ByteBuffer) when the project is compiled with newer
     * Java version and run on Java 8.
     * <p/>
     * <a href="https://docs.oracle.com/javase/8/docs/api/java/nio/ByteBuffer.html">Java 8</a> doesn't provide override the
     * following Buffer methods in subclasses:
     *
     * <pre>
     * Buffer clear​()
     * Buffer flip​()
     * Buffer limit​(int newLimit)
     * Buffer mark​()
     * Buffer position​(int newPosition)
     * Buffer reset​()
     * Buffer rewind​()
     * </pre>
     *
     * <a href="https://docs.oracle.com/javase/9/docs/api/java/nio/ByteBuffer.html">Java 9</a> introduces the overrides in child
     * classes (e.g the ByteBuffer), but the return type is the specialized one and not the abstract {@link Buffer}. So the code
     * compiled with newer Java is not working on Java 8 unless a workaround with explicit casting is used.
     *
     * @param buf buffer to cast to the abstract {@link Buffer} parent type
     * @return the provided buffer
     */
    public static Buffer upcast(Buffer buf) {
        return buf;
    }

    public static void put(ByteBuffer dst, ByteBuffer src) {
        int readableBytes = src.remaining();
        int writableBytes = dst.remaining();
        if (readableBytes <= writableBytes) {
            // there is enough space in the dst buffer to copy the src
            dst.put(src);
        } else {
            // there is not enough space in the dst buffer, so we need to
            // copy as much as we can.
            int srcOldLimit = src.limit();
            src.limit(src.position() + writableBytes);
            dst.put(src);
            src.limit(srcOldLimit);
        }
    }

}