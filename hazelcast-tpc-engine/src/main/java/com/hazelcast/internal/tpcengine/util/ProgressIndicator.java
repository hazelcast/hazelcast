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

package com.hazelcast.internal.tpcengine.util;


import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * A progress indicator is a counter that is updated by one thread, and can be read by many and is
 * useful to indicate progress like number of bytes received on a socket.
 * <p/>
 * The value is guaranteed to be atomic, coherent and progress is guaranteed as well. But no ordering guarantees
 * with respect to other loads/stores are provided.
 */
public class ProgressIndicator {

    private static final Unsafe UNSAFE = UnsafeLocator.UNSAFE;
    private static final long OFFSET;

    static {
        Field field = null;
        try {
            field = ProgressIndicator.class.getDeclaredField("value");
        } catch (NoSuchFieldException ignore) {
            ExceptionUtil.ignore(ignore);
        }
        OFFSET = UNSAFE.objectFieldOffset(field);
    }

    private volatile long value;

    /**
     * Creates a new ProgressIndicator with 0 as initial value.
     */
    public ProgressIndicator() {
    }

    /**
     * Increases the current value by 1.
     *
     * @return the updated value.
     */
    @SuppressWarnings("checkstyle:innerassignment")
    public long inc() {
        final long newLocalValue = value + 1;
        // In the future we could use an opaque write.
        UNSAFE.putOrderedLong(this, OFFSET, newLocalValue);
        return newLocalValue;
    }

    /**
     * Increases the current value by delta (delta can be 0 or negative).
     *
     * @param delta the amount to add.
     * @return the updated value.
     */
    @SuppressWarnings("checkstyle:innerassignment")
    public long inc(long delta) {
        final long newLocalValue = value + delta;
        // In the future we could use an opaque write.
        UNSAFE.putOrderedLong(this, OFFSET, newLocalValue);
        return newLocalValue;
    }

    /**
     * Gets the current value.
     *
     * @return the current value.
     */
    public long get() {
        // In the future we could use an opaque read.
        return value;
    }

    @Override
    public String toString() {
        return "" + value;
    }
}
