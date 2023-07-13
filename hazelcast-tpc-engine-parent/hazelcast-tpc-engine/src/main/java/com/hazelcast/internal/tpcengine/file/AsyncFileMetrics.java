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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Contains the metrics for the {@link AsyncFile}.
 * <p/>
 * The metrics should only be updated by the event loop thread, but can be read by any thread.
 */
@SuppressWarnings("checkstyle:ConstantName")
public final class AsyncFileMetrics {

    private static final VarHandle NOPS;
    private static final VarHandle READS;
    private static final VarHandle WRITES;
    private static final VarHandle FSYNCS;
    private static final VarHandle FDATASYNCS;
    private static final VarHandle BYTES_READ;
    private static final VarHandle BYTES_WRITTEN;

    private volatile long nops;
    private volatile long reads;
    private volatile long writes;
    private volatile long fsyncs;
    private volatile long fdatasyncs;
    private volatile long bytesRead;
    private volatile long bytesWritten;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            NOPS = l.findVarHandle(AsyncFileMetrics.class, "nops", long.class);
            READS = l.findVarHandle(AsyncFileMetrics.class, "reads", long.class);
            WRITES = l.findVarHandle(AsyncFileMetrics.class, "writes", long.class);
            FSYNCS = l.findVarHandle(AsyncFileMetrics.class, "fsyncs", long.class);
            FDATASYNCS = l.findVarHandle(AsyncFileMetrics.class, "fdatasyncs", long.class);
            BYTES_READ = l.findVarHandle(AsyncFileMetrics.class, "bytesRead", long.class);
            BYTES_WRITTEN = l.findVarHandle(AsyncFileMetrics.class, "bytesWritten", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Returns the number of read operations that have been successfully performed on the file.
     */
    public long reads() {
        return (long) READS.getOpaque(this);
    }

    public void incReads() {
        READS.setOpaque(this, (long) READS.getOpaque(this) + 1);
    }

    /**
     * Returns the number of write operations that have been successfully performed on the file.
     */
    public long writes() {
        return (long) WRITES.getOpaque(this);
    }

    public void incWrites() {
        WRITES.setOpaque(this, (long) WRITES.getOpaque(this) + 1);
    }

    /**
     * Returns the number of nop operations that have been successfully performed on the file.
     */
    public long nops() {
        return (long) NOPS.getOpaque(this);
    }

    public void incNops() {
        NOPS.setOpaque(this, (long) NOPS.getOpaque(this) + 1);
    }

    /**
     * Returns the number of fsyncs that have been successfully called on the file.
     */
    public long fsyncs() {
        return (long) FSYNCS.getOpaque(this);
    }

    public void incFsyncs() {
        FSYNCS.setOpaque(this, (long) FSYNCS.getOpaque(this) + 1);
    }

    /**
     * Returns the number of fdatasyncs that have been successfully called on the file.
     */
    public long fdatasyncs() {
        return (long) FDATASYNCS.getOpaque(this);
    }

    public void incFdatasyncs() {
        FDATASYNCS.setOpaque(this, (long) FDATASYNCS.getOpaque(this) + 1);
    }

    /**
     * Returns the number bytes that have been successfully written to the file.
     */
    public long bytesWritten() {
        return (long) BYTES_WRITTEN.getOpaque(this);
    }

    public void incBytesWritten(int amount) {
        BYTES_WRITTEN.setOpaque(this, (long) BYTES_WRITTEN.getOpaque(this) + amount);
    }

    /**
     * Returns the number of bytes that have been successfully read from the file.
     */
    public long bytesRead() {
        return (long) BYTES_READ.getOpaque(this);
    }

    public void incBytesRead(int amount) {
        BYTES_READ.setOpaque(this, (long) BYTES_READ.getOpaque(this) + amount);
    }
}
