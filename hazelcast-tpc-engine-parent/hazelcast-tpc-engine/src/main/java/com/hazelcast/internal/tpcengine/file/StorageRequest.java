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

package com.hazelcast.internal.tpcengine.file;

import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.util.IntBiConsumer;

/**
 * Represents a request to file like a read or write.
 * <p/>
 * StorageRequest should be pooled by the {@link StorageScheduler} to avoid
 * litter.
 * <p/>
 * BlockRequests are 'generic' in the sense that the same BlockRequest object
 * can be interpreted in different ways depending on the opcode. So there
 * are no different subclasses for e.g. a read or a write. Unlike C, Java
 * doesn't have support for unions and hence we end up with this approach. Having
 * a single type makes makes it a lot easier to pool N instances since just a
 * single pool needs to be maintained instead of a pool of N instances for every
 * type.
 * <p/>
 * StorageRequest can be subclassed by the {@link StorageScheduler} to add
 * additional fields or behaviors.
 * <p/>
 * An instance fo the StorageRequest is obtained through the
 * {@link StorageScheduler#allocate()} and then scheduled through the
 * {@link StorageScheduler#schedule(StorageRequest)} method.
 */
@SuppressWarnings({"checkstyle:VisibilityModifier"})
public class StorageRequest {
    public static final int STR_REQ_OP_NOP = 1;
    public static final int STR_REQ_OP_READ = 2;
    public static final int STR_REQ_OP_WRITE = 3;
    public static final int STR_REQ_OP_FSYNC = 4;
    public static final int STR_REQ_OP_FDATASYNC = 5;
    public static final int STR_REQ_OP_OPEN = 6;
    public static final int STR_REQ_OP_CLOSE = 7;
    public static final int STR_REQ_OP_FALLOCATE = 8;

    public IOBuffer buffer;
    public int permissions;
    public AsyncFile file;
    public long offset;
    public int length;
    public byte opcode;
    public int flags;
    public int rwFlags;
    public IntBiConsumer<Throwable> callback;

    @SuppressWarnings({"checkstyle:ReturnCount"})
    public static String storageReqOpcodeToString(int opcode) {
        switch (opcode) {
            case STR_REQ_OP_NOP:
                return "STR_REQ_OP_NOP";
            case STR_REQ_OP_READ:
                return "STR_REQ_OP_READ";
            case STR_REQ_OP_WRITE:
                return "STR_REQ_OP_WRITE";
            case STR_REQ_OP_FSYNC:
                return "STR_REQ_OP_FSYNC";
            case STR_REQ_OP_FDATASYNC:
                return "STR_REQ_OP_FDATASYNC";
            case STR_REQ_OP_OPEN:
                return "STR_REQ_OP_OPEN";
            case STR_REQ_OP_CLOSE:
                return "STR_REQ_OP_CLOSE";
            case STR_REQ_OP_FALLOCATE:
                return "STR_REQ_OP_FALLOCATE";
            default:
                return "unknown-opcode(" + opcode + ")";
        }
    }

    @Override
    public String toString() {
        return "StorageRequest{"
                + "file=" + file
                + ", offset=" + offset
                + ", length=" + length
                + ", opcode=" + storageReqOpcodeToString(opcode)
                + ", flags=" + flags
                + ", permissions=" + permissions
                + ", rwFlags=" + rwFlags
                + ", buf=" + (buffer == null ? "null" : buffer.toDebugString())
                + "}";
    }
}
