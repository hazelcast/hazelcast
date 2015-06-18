/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.Logger;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Unsafe accessor.
 * <p/>
 * Warning: Although both array base-offsets and array index-scales are constant over time currently,
 * a later JVM implementation can change this behaviour to allow varying index-scales and base-offsets
 * over time or per array instances (e.g. compressed primitive arrays or backwards growing arrays...)
 * <p/>
 * See Gil Tene's comment related to Unsafe usage;
 * https://groups.google.com/d/msg/mechanical-sympathy/X-GtLuG0ETo/LMV1d_2IybQJ
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

    public static final int MEM_COPY_THRESHOLD = 1024 * 1024;

    private static final String UNSAFE_MODE_PROPERTY_NAME = "hazelcast.unsafe.mode";
    private static final String UNSAFE_EXPLICITLY_DISABLED = "disabled";
    private static final String UNSAFE_EXPLICITLY_ENABLED = "enforced";

    private static final String UNSAFE_WARNING_WHEN_NOT_FOUND = "sun.misc.Unsafe isn't available, some features might "
            + "be not available";
    private static final String UNSAFE_WARNING_WHEN_EXPLICTLY_DISABLED = "sun.misc.Unsafe has been disabled "
            + "via System Property " + UNSAFE_MODE_PROPERTY_NAME + ", some features might be not available.";
    private static final String UNSAFE_WARNING_WHEN_UNALIGNED_ACCESS_NOT_ALLOWED = "sun.misc.Unsafe has been disabled "
            + "because your platform does not support unaligned access to memory, some features might be not available.";
    private static final String UNSAFE_WARNING_WHEN_ENFORCED_ON_PLATFORM_WHERE_NOT_SUPPORTED = "You platform does not "
            + "seem to support unaligned access to memory. Unsafe usage has been enforced via System Property "
            + UNSAFE_MODE_PROPERTY_NAME + " This is not a supported configuration and it can crash JVM or corrupt your data!";


    static {
        Unsafe unsafe;
        try {
            unsafe = findUnsafeIfAllowed();
        } catch (RuntimeException e) {
            unsafe = null;
        }
        UNSAFE = unsafe;

        BYTE_ARRAY_BASE_OFFSET = arrayBaseOffset(byte[].class, unsafe);
        SHORT_ARRAY_BASE_OFFSET = arrayBaseOffset(short[].class, unsafe);
        CHAR_ARRAY_BASE_OFFSET = arrayBaseOffset(char[].class, unsafe);
        INT_ARRAY_BASE_OFFSET = arrayBaseOffset(int[].class, unsafe);
        FLOAT_ARRAY_BASE_OFFSET = arrayBaseOffset(float[].class, unsafe);
        LONG_ARRAY_BASE_OFFSET = arrayBaseOffset(long[].class, unsafe);
        DOUBLE_ARRAY_BASE_OFFSET = arrayBaseOffset(double[].class, unsafe);

        BYTE_ARRAY_INDEX_SCALE = arrayIndexScale(byte[].class, unsafe);
        SHORT_ARRAY_INDEX_SCALE = arrayIndexScale(short[].class, unsafe);
        CHAR_ARRAY_INDEX_SCALE = arrayIndexScale(char[].class, unsafe);
        INT_ARRAY_INDEX_SCALE = arrayIndexScale(int[].class, unsafe);
        FLOAT_ARRAY_INDEX_SCALE = arrayIndexScale(float[].class, unsafe);
        LONG_ARRAY_INDEX_SCALE = arrayIndexScale(long[].class, unsafe);
        DOUBLE_ARRAY_INDEX_SCALE = arrayIndexScale(double[].class, unsafe);

        boolean unsafeAvailable = false;
        try {
            // test if unsafe has required methods...
            if (unsafe != null) {
                byte[] buffer = new byte[8];
                unsafe.putChar(buffer, BYTE_ARRAY_BASE_OFFSET, '0');
                unsafe.putShort(buffer, BYTE_ARRAY_BASE_OFFSET, (short) 1);
                unsafe.putInt(buffer, BYTE_ARRAY_BASE_OFFSET, 2);
                unsafe.putFloat(buffer, BYTE_ARRAY_BASE_OFFSET, 3f);
                unsafe.putLong(buffer, BYTE_ARRAY_BASE_OFFSET, 4L);
                unsafe.putDouble(buffer, BYTE_ARRAY_BASE_OFFSET, 5d);
                unsafe.copyMemory(new byte[8], BYTE_ARRAY_BASE_OFFSET, buffer, BYTE_ARRAY_BASE_OFFSET, buffer.length);

                unsafeAvailable = true;
            }
        } catch (Throwable e) {
            Logger.getLogger(UnsafeHelper.class).warning(UNSAFE_WARNING_WHEN_NOT_FOUND);
        }

        UNSAFE_AVAILABLE = unsafeAvailable;
    }

    private UnsafeHelper() {
    }

    private static long arrayBaseOffset(Class<?> type, Unsafe unsafe) {
        return unsafe == null ? -1 : unsafe.arrayBaseOffset(type);
    }

    private static int arrayIndexScale(Class<?> type, Unsafe unsafe) {
        return unsafe == null ? -1 : unsafe.arrayIndexScale(type);
    }

    private static Unsafe findUnsafeIfAllowed() {
        if (isUnsafeExplicitlyDisabled()) {
            Logger.getLogger(UnsafeHelper.class).warning(UNSAFE_WARNING_WHEN_EXPLICTLY_DISABLED);
            return null;
        }
        if (!isUnalignedAccessAllowed()) {
            if (isUnsafeExplicitlyEnforced()) {
                Logger.getLogger(UnsafeHelper.class).warning(UNSAFE_WARNING_WHEN_ENFORCED_ON_PLATFORM_WHERE_NOT_SUPPORTED);
            } else {
                Logger.getLogger(UnsafeHelper.class).warning(UNSAFE_WARNING_WHEN_UNALIGNED_ACCESS_NOT_ALLOWED);
                return null;
            }
        }
        Unsafe unsafe = findUnsafe();
        if (unsafe == null) {
            Logger.getLogger(UnsafeHelper.class).warning(UNSAFE_WARNING_WHEN_NOT_FOUND);
        }
        return unsafe;
    }

    private static boolean isUnsafeExplicitlyDisabled() {
        String mode = System.getProperty(UNSAFE_MODE_PROPERTY_NAME);
        return UNSAFE_EXPLICITLY_DISABLED.equals(mode);
    }

    private static boolean isUnsafeExplicitlyEnforced() {
        String mode = System.getProperty(UNSAFE_MODE_PROPERTY_NAME);
        return UNSAFE_EXPLICITLY_ENABLED.equals(mode);
    }

    private static boolean isUnalignedAccessAllowed() {
        //we can't use Unsafe to access memory on platforms where unaligned access is not allowed
        //see https://github.com/hazelcast/hazelcast/issues/5518 for details.
        String arch = System.getProperty("os.arch");
        //list of architectures copied from OpenJDK - java.nio.Bits::unaligned
        boolean unalignedAllowed = arch.equals("i386") || arch.equals("x86")
                || arch.equals("amd64") || arch.equals("x86_64");
        return unalignedAllowed;
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
                    } catch (Exception e) {
                        throw new RuntimeException("Unsafe unavailable", e);
                    }
                    throw new RuntimeException("Unsafe unavailable");
                }
            });
        }
    }
}
