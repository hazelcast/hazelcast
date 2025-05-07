/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.net;

import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.util.ReflectionUtil;

import java.lang.invoke.VarHandle;

/**
 * Contains the metrics for an {@link AsyncSocket}.
 */
@SuppressWarnings("checkstyle:ConstantName")
public class AsyncSocketMetrics {

    private static final VarHandle BYTES_READ = ReflectionUtil.findVarHandle("bytesRead", long.class);
    private static final VarHandle BYTES_WRITTEN = ReflectionUtil.findVarHandle("bytesWritten", long.class);
    private static final VarHandle WRITE_EVENTS = ReflectionUtil.findVarHandle("writeEvents", long.class);
    private static final VarHandle READ_EVENTS = ReflectionUtil.findVarHandle("readEvents", long.class);

    private volatile long bytesRead;
    private volatile long bytesWritten;
    private volatile long writeEvents;
    private volatile long readEvents;

    /**
     * Returns bytes read.
     *
     * @return bytes read.
     */
    public long bytesRead() {
        return (long) BYTES_READ.getOpaque(this);
    }

    /**
     * Increases the bytes read.
     *
     * @param delta the amount to increase.
     */
    public void incBytesRead(long delta) {
        BYTES_READ.setOpaque(this, (long) BYTES_READ.getOpaque(this) + delta);
    }

    /**
     * Returns the bytes written.
     *
     * @return the bytes written.
     */
    public long bytesWritten() {
        return (long) BYTES_WRITTEN.getOpaque(this);
    }

    /**
     * Increases the bytes written.
     *
     * @param delta the amount to increase.
     */
    public void incBytesWritten(long delta) {
        BYTES_WRITTEN.setOpaque(this, (long) BYTES_WRITTEN.getOpaque(this) + delta);
    }

    /**
     * Returns the number of write events. So the number of times the {@link AsyncSocket}
     * was scheduled on the {@link Reactor} for writing purposes.
     *
     * @return number of write events.
     */
    public long writeEvents() {
        return (long) WRITE_EVENTS.getOpaque(this);
    }

    /**
     * Increases the number of write events by 1.
     */
    public void incWriteEvents() {
        WRITE_EVENTS.setOpaque(this, (long) WRITE_EVENTS.getOpaque(this) + 1);
    }

    /**
     * Returns the number of read events. So the number of times the {@link AsyncSocket}
     * was scheduled on the {@link Reactor} for reading purposes.
     *
     * @return number of read events.
     */
    public long readEvents() {
        return (long) READ_EVENTS.getOpaque(this);
    }

    /**
     * Increases the number of read events by 1.
     */
    public void incReadEvents() {
        READ_EVENTS.setOpaque(this, (long) READ_EVENTS.getOpaque(this) + 1);
    }
}
