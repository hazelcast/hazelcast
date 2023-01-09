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

import java.io.Closeable;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Collection;

public class IOUtil {

    public static final int SIZEOF_INT = Integer.BYTES;
    public static final int SIZEOF_LONG = Long.BYTES;
    public static final int SIZEOF_CHAR = Character.BYTES;
    public static final int SIZEOF_BYTE = 1;

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

    public static void closeResources(Collection<? extends Closeable> collection) {
        if (collection == null) {
            return;
        }
        for (Closeable closeable : collection) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (IOException e) {
                }
            }
        }
    }

    public static void put(ByteBuffer dst, ByteBuffer src) {
        if (src.remaining() <= dst.remaining()) {
            // there is enough space in the dst buffer to copy the src
            dst.put(src);
        } else {
            // there is not enough space in the dst buffer, so we need to
            // copy as much as we can.
            int srcOldLimit = src.limit();
            src.limit(src.position() + dst.remaining());
            dst.put(src);
            src.limit(srcOldLimit);
        }
    }
}
