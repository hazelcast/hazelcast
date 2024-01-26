/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.util;

import java.nio.ByteBuffer;

public final class BufferUtil {

    private BufferUtil() {
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
            bb.clear();
        }
    }

    /**
     * Allocates a new {@link ByteBuffer}.
     *
     * @param direct if the ByteBuffer should be a direct ByteBuffer.
     * @param capacity the capacity of the ByteBuffer to allocate
     * @return the allocated ByteBuffer.
     */
    public static ByteBuffer allocateBuffer(boolean direct, int capacity) {
        return direct ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
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
