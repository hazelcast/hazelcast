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

package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.tpcengine.util.UnsafeLocator;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Contains the metrics for an {@link AsyncSocket}.
 */
@SuppressWarnings("checkstyle:ConstantName")
public class AsyncSocketMetrics {
    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;
    private static final long OFFSET_bytesRead;
    private static final long OFFSET_bytesWritten;
    private static final long OFFSET_writeEvents;
    private static final long OFFSET_readEvents;

    private volatile long bytesRead;
    private volatile long bytesWritten;
    private volatile long writeEvents;
    private volatile long readEvents;

    static {
        try {
            OFFSET_bytesRead = getOffset("bytesRead");
            OFFSET_bytesWritten = getOffset("bytesWritten");
            OFFSET_writeEvents = getOffset("writeEvents");
            OFFSET_readEvents = getOffset("readEvents");
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private static long getOffset(String fieldName) throws NoSuchFieldException {
        Field field = AsyncSocketMetrics.class.getDeclaredField(fieldName);
        return UNSAFE.objectFieldOffset(field);
    }

    /**
     * Returns bytes read.
     *
     * @return bytes read.
     */
    public long bytesRead() {
        return bytesRead;
    }

    /**
     * Increases the bytes read.
     *
     * @param delta the amount to increase.
     */
    public void incBytesRead(long delta) {
        UNSAFE.putOrderedLong(this, OFFSET_bytesRead, bytesRead + delta);
    }

    /**
     * Returns the bytes written.
     *
     * @return the bytes written.
     */
    public long bytesWritten() {
        return bytesWritten;
    }

    /**
     * Increases the bytes written.
     *
     * @param delta the amount to increase.
     */
    public void incBytesWritten(long delta) {
        UNSAFE.putOrderedLong(this, OFFSET_bytesWritten, bytesWritten + delta);
    }

    /**
     * Returns the number of write events. So the number of times the {@link AsyncSocket}
     * was scheduled on the {@link Reactor} for writing purposes.
     *
     * @return number of write events.
     */
    public long writeEvents() {
        return writeEvents;
    }

    /**
     * Increases the number of write events by 1.
     */
    public void incWriteEvents() {
        UNSAFE.putOrderedLong(this, OFFSET_writeEvents, writeEvents + 1);
    }

    /**
     * Returns the number of read events. So the number of times the {@link AsyncSocket}
     * was scheduled on the {@link Reactor} for reading purposes.
     *
     * @return number of read events.
     */
    public long readEvents() {
        return readEvents;
    }

    /**
     * Increases the number of read events by 1.
     */
    public void incReadEvents() {
        UNSAFE.putOrderedLong(this, OFFSET_readEvents, readEvents + 1);
    }
}
