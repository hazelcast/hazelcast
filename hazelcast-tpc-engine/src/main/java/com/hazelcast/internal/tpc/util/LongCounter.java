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

public class LongCounter {

    private static final Unsafe UNSAFE = UnsafeUtil.UNSAFE;
    private static final long OFFSET;

    static {
        Field field = null;
        try {
            field = LongCounter.class.getDeclaredField("value");
        } catch (NoSuchFieldException ignore) {
            Util.ignore(ignore);
        }
        OFFSET = UNSAFE.objectFieldOffset(field);
    }

    private volatile long value;

    public LongCounter() {
    }

    @SuppressWarnings("checkstyle:innerassignment")
    public long inc() {
        final long newLocalValue = value + 1;
        UNSAFE.putOrderedLong(this, OFFSET, newLocalValue);
        return newLocalValue;
    }

    @SuppressWarnings("checkstyle:innerassignment")
    public long inc(long amount) {
        final long newLocalValue = value + amount;
        UNSAFE.putOrderedLong(this, OFFSET, newLocalValue);
        return newLocalValue;
    }

    public long get() {
        return value;
    }

    @Override
    public String toString() {
        return "" + value;
    }
}
