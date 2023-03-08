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

import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Contains the metrics for the {@link AsyncFile}.
 */
@SuppressWarnings("checkstyle:ConstantName")
public final class AsyncFileMetrics {

    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;
    private static final long OFFSET_reads;
    private static final long OFFSET_writes;
    private static final long OFFSET_nops;
    private static final long OFFSET_fsyncs;
    private static final long OFFSET_fdatasyncs;
    private static final long OFFSET_bytesRead;
    private static final long OFFSET_bytesWritten;

    private volatile long nops;
    private volatile long reads;
    private volatile long writes;
    private volatile long fsyncs;
    private volatile long fdatasyncs;
    private volatile long bytesRead;
    private volatile long bytesWritten;

    static {
        try {
            OFFSET_reads = getOffset("reads");
            OFFSET_writes = getOffset("writes");
            OFFSET_nops = getOffset("nops");
            OFFSET_fsyncs = getOffset("fsyncs");
            OFFSET_fdatasyncs = getOffset("fdatasyncs");
            OFFSET_bytesRead = getOffset("bytesRead");
            OFFSET_bytesWritten = getOffset("bytesWritten");
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private static long getOffset(String fieldName) throws NoSuchFieldException {
        Field field = AsyncFileMetrics.class.getDeclaredField(fieldName);
        return UNSAFE.objectFieldOffset(field);
    }

    /**
     * Returns the number of read operations that have been performed on the file.
     */
    public long reads() {
        return reads;
    }

    public void incReads() {
        UNSAFE.putOrderedLong(this, OFFSET_reads, reads + 1);
    }

    /**
     * Returns the number of write operations that have been performed on the file.
     */
    public long writes() {
        return writes;
    }

    public void incWrites() {
        UNSAFE.putOrderedLong(this, OFFSET_writes, writes + 1);
    }

    /**
     * Returns the number of nop operations that have been performed on the file.
     */
    public long nops() {
        return nops;
    }

    public void incNops() {
        UNSAFE.putOrderedLong(this, OFFSET_nops, nops + 1);
    }

    /**
     * Returns the number of fsyncs that have been called on the file.
     */
    public long fsyncs() {
        return fsyncs;
    }

    public void incFsyncs() {
        UNSAFE.putOrderedLong(this, OFFSET_fsyncs, fsyncs + 1);
    }

    /**
     * Returns the number of fdatasyncs that have been called on the file.
     */
    public long fdatasyncs() {
        return fdatasyncs;
    }

    public void incFdatasyncs() {
        UNSAFE.putOrderedLong(this, OFFSET_fdatasyncs, fdatasyncs + 1);
    }

    /**
     * Returns the number bytes that have been written to the file.
     */
    public long bytesWritten() {
        return bytesWritten;
    }

    public void incBytesWritten(int amount) {
        UNSAFE.putOrderedLong(this, OFFSET_bytesWritten, bytesWritten + amount);
    }

    /**
     * Returns the number of bytes that have bee read from the file.
     */
    public long bytesRead() {
        return bytesRead;
    }

    public void incBytesRead(int amount) {
        UNSAFE.putOrderedLong(this, OFFSET_bytesRead, bytesRead + amount);
    }
}
