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

package com.hazelcast.internal.memory.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.internal.nio.Bits;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.QuickMath.normalize;

/**
 * Utility class for {@link sun.misc.Unsafe}.
 */
public final class UnsafeUtil {

    /**
     * If this constant is {@code true}, then {@link #UNSAFE} refers to a usable {@link sun.misc.Unsafe} instance.
     */
    public static final boolean UNSAFE_AVAILABLE;

    /**
     * The {@link sun.misc.Unsafe} instance which is available and ready to use.
     */
    public static final Unsafe UNSAFE;

    private static final ILogger LOGGER = Logger.getLogger(UnsafeUtil.class);

    static {
        Unsafe unsafe;
        try {
            unsafe = findUnsafe();
            if (unsafe != null) {
                // test if unsafe has required methods...
                checkUnsafeInstance(unsafe);
            }
        } catch (Throwable t) {
            unsafe = null;
            logFailureToFindUnsafeDueTo(t);
        }
        UNSAFE = unsafe;
        UNSAFE_AVAILABLE = UNSAFE != null;
    }

    private UnsafeUtil() {
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
                                if (type.isAssignableFrom(field.getType())) {
                                    field.setAccessible(true);
                                    return type.cast(field.get(type));
                                }
                            }
                        }
                    } catch (Throwable t) {
                        throw rethrow(t);
                    }
                    throw new RuntimeException("Unsafe unavailable");
                }
            });
        }
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private static void checkUnsafeInstance(Unsafe unsafe) {
        long arrayBaseOffset = unsafe.arrayBaseOffset(byte[].class);
        byte[] buffer = new byte[(int) arrayBaseOffset + (2 * Bits.LONG_SIZE_IN_BYTES)];
        unsafe.putByte(buffer, arrayBaseOffset, (byte) 0x00);
        unsafe.putBoolean(buffer, arrayBaseOffset, false);
        unsafe.putChar(buffer, normalize(arrayBaseOffset, Bits.CHAR_SIZE_IN_BYTES), '0');
        unsafe.putShort(buffer, normalize(arrayBaseOffset, Bits.SHORT_SIZE_IN_BYTES), (short) 1);
        unsafe.putInt(buffer, normalize(arrayBaseOffset, Bits.INT_SIZE_IN_BYTES), 2);
        unsafe.putFloat(buffer, normalize(arrayBaseOffset, Bits.FLOAT_SIZE_IN_BYTES), 3f);
        unsafe.putLong(buffer, normalize(arrayBaseOffset, Bits.LONG_SIZE_IN_BYTES), 4L);
        unsafe.putDouble(buffer, normalize(arrayBaseOffset, Bits.DOUBLE_SIZE_IN_BYTES), 5d);
        unsafe.copyMemory(new byte[buffer.length], arrayBaseOffset, buffer, arrayBaseOffset, buffer.length);
    }

    private static void logFailureToFindUnsafeDueTo(final Throwable reason) {
        if (LOGGER.isFinestEnabled()) {
            LOGGER.finest("Unable to get an instance of Unsafe. Unsafe-based operations will be unavailable", reason);
        } else {
            LOGGER.warning("Unable to get an instance of Unsafe. Unsafe-based operations will be unavailable");
        }
    }
}
