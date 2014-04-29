/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio;

import com.hazelcast.core.HazelcastException;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Unsafe accessor.
 *
 * Warning: Although both array base-offsets and array index-scales are
 * constant over time currently,
 * a later JVM implementation can change this behaviour
 * to allow varying index-scales and base-offsets over time or per array instances.
 * (For example; compressed primitive arrays or backwards growing arrays...)
 *
 * See Gil Tene's comment related to Unsafe usage;
 * https://groups.google.com/d/msg/mechanical-sympathy/X-GtLuG0ETo/LMV1d_2IybQJ
 *
 * @author mdogan 6/14/13
 */
public final class UnsafeHelper {

    public static final Unsafe UNSAFE;
    public static final boolean UNSAFE_AVAILABLE;

    public static final long BYTE_ARRAY_BASE_OFFSET;
    public static final long SHORT_ARRAY_BASE_OFFSET;
    public static final long CHAR_ARRAY_BASE_OFFSET;
    public static final long INT_ARRAY_BASE_OFFSET;
    public static final long FLOAT_ARRAY_BASE_OFFSET;
    public static final long LONG_ARRAY_BASE_OFFSET;
    public static final long DOUBLE_ARRAY_BASE_OFFSET;

    public static final int BYTE_ARRAY_INDEX_SCALE;
    public static final int SHORT_ARRAY_INDEX_SCALE;
    public static final int CHAR_ARRAY_INDEX_SCALE;
    public static final int INT_ARRAY_INDEX_SCALE;
    public static final int FLOAT_ARRAY_INDEX_SCALE;
    public static final int LONG_ARRAY_INDEX_SCALE;
    public static final int DOUBLE_ARRAY_INDEX_SCALE;

    static {
        try {
            Unsafe unsafe = findUnsafe();

            BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);
            SHORT_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(short[].class);
            CHAR_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(char[].class);
            INT_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(int[].class);
            FLOAT_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(float[].class);
            LONG_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(long[].class);
            DOUBLE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(double[].class);

            BYTE_ARRAY_INDEX_SCALE = unsafe.arrayIndexScale(byte[].class);
            SHORT_ARRAY_INDEX_SCALE = unsafe.arrayIndexScale(short[].class);
            CHAR_ARRAY_INDEX_SCALE = unsafe.arrayIndexScale(char[].class);
            INT_ARRAY_INDEX_SCALE = unsafe.arrayIndexScale(int[].class);
            FLOAT_ARRAY_INDEX_SCALE = unsafe.arrayIndexScale(float[].class);
            LONG_ARRAY_INDEX_SCALE = unsafe.arrayIndexScale(long[].class);
            DOUBLE_ARRAY_INDEX_SCALE = unsafe.arrayIndexScale(double[].class);

            // test if unsafe has required methods...
            byte[] buffer = new byte[8];
            unsafe.putChar(buffer, BYTE_ARRAY_BASE_OFFSET, '0');
            unsafe.putShort(buffer, BYTE_ARRAY_BASE_OFFSET, (short) 1);
            unsafe.putInt(buffer, BYTE_ARRAY_BASE_OFFSET, 2);
            unsafe.putFloat(buffer, BYTE_ARRAY_BASE_OFFSET, 3f);
            unsafe.putLong(buffer, BYTE_ARRAY_BASE_OFFSET, 4L);
            unsafe.putDouble(buffer, BYTE_ARRAY_BASE_OFFSET, 5d);
            unsafe.copyMemory(new byte[8], BYTE_ARRAY_BASE_OFFSET, buffer, BYTE_ARRAY_BASE_OFFSET, buffer.length);

            UNSAFE = unsafe;
            UNSAFE_AVAILABLE = UNSAFE != null;
        } catch (Throwable e) {
            throw new HazelcastException(e);
        }
    }

    private UnsafeHelper() {
    }

    private static Unsafe findUnsafe() {
        try {
            return Unsafe.getUnsafe();
        } catch (SecurityException se) {
            return AccessController.doPrivileged(new PrivilegedAction<Unsafe>() {
                @Override
                public Unsafe run() {
                    try {
                        Class<Unsafe> type = Unsafe.class;
                        try {
                            Field field = type.getDeclaredField("theUnsafe");
                            field.setAccessible(true);
                            return type.cast(field.get(type));

                        } catch (Exception e) {
                            for (Field field : type.getDeclaredFields()) {
                                if (type.isAssignableFrom(field.getType()))  {
                                    field.setAccessible(true);
                                    return type.cast(field.get(type));
                                }
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Unsafe unavailable", e);
                    }
                    throw new RuntimeException("Unsafe unavailable");
                }
            });
        }
    }
}
