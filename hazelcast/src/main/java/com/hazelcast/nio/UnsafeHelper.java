/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.impl.AlignmentUtil;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.annotation.PrivateApi;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

import static com.hazelcast.util.QuickMath.normalize;

/**
 * Unsafe accessor.
 * <p/>
 * Warning: Although both array base-offsets and array index-scales are constant over time currently,
 * a later JVM implementation can change this behaviour to allow varying index-scales and base-offsets
 * over time or per array instances (e.g. compressed primitive arrays or backwards growing arrays...)
 * <p/>
 * <p>
 * See Gil Tene's comment related to Unsafe usage;
 * https://groups.google.com/d/msg/mechanical-sympathy/X-GtLuG0ETo/LMV1d_2IybQJ
 * </p>
 * @deprecated Use {@link MemoryAccessor} instead due to following reasons:
 * <p>
 * Deprecated to {@link MemoryAccessor} due to following reasons:
 * <ul>
 *     <li>
 *          Preventing hard-dependency to {@link sun.misc.Unsafe}/
 *     </li>
 *     <li>
 *          Some platforms (such as SPARC) don't support unaligned memory access.
 *          So on these platforms memory access alignments must be checked and handled if possible.
 *     </li>
 * </ul>
 * </p>
 */
@Deprecated
@PrivateApi
public final class UnsafeHelper {

    /**
     * @deprecated {@link GlobalMemoryAccessorRegistry#MEM} instead
     */
    @Deprecated
    public static final Unsafe UNSAFE;

    /**
     * @deprecated {@link GlobalMemoryAccessorRegistry#MEM_AVAILABLE} instead
     */
    @Deprecated
    public static final boolean UNSAFE_AVAILABLE;

    public static final long BYTE_ARRAY_BASE_OFFSET;
    public static final long BOOLEAN_ARRAY_BASE_OFFSET;
    public static final long SHORT_ARRAY_BASE_OFFSET;
    public static final long CHAR_ARRAY_BASE_OFFSET;
    public static final long INT_ARRAY_BASE_OFFSET;
    public static final long FLOAT_ARRAY_BASE_OFFSET;
    public static final long LONG_ARRAY_BASE_OFFSET;

    public static final long DOUBLE_ARRAY_BASE_OFFSET;
    public static final int BOOLEAN_ARRAY_INDEX_SCALE;
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
        BOOLEAN_ARRAY_BASE_OFFSET = arrayBaseOffset(boolean[].class, unsafe);
        SHORT_ARRAY_BASE_OFFSET = arrayBaseOffset(short[].class, unsafe);
        CHAR_ARRAY_BASE_OFFSET = arrayBaseOffset(char[].class, unsafe);
        INT_ARRAY_BASE_OFFSET = arrayBaseOffset(int[].class, unsafe);
        FLOAT_ARRAY_BASE_OFFSET = arrayBaseOffset(float[].class, unsafe);
        LONG_ARRAY_BASE_OFFSET = arrayBaseOffset(long[].class, unsafe);
        DOUBLE_ARRAY_BASE_OFFSET = arrayBaseOffset(double[].class, unsafe);

        BYTE_ARRAY_INDEX_SCALE = arrayIndexScale(byte[].class, unsafe);
        BOOLEAN_ARRAY_INDEX_SCALE = arrayIndexScale(boolean[].class, unsafe);
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
                long arrayBaseOffset = unsafe.arrayBaseOffset(byte[].class);
                byte[] buffer = new byte[(int) arrayBaseOffset + (2 * Bits.LONG_SIZE_IN_BYTES)];
                unsafe.putByte(buffer, arrayBaseOffset, (byte) 0x00);
                unsafe.putBoolean(buffer, arrayBaseOffset, false);
                unsafe.putChar(buffer, normalize(arrayBaseOffset, Bits.CHAR_SIZE_IN_BYTES), '0');
                unsafe.putShort(buffer, normalize(arrayBaseOffset, Bits.SHORT_SIZE_IN_BYTES), (short) 1);
                unsafe.putInt(buffer, normalize(arrayBaseOffset, Bits.INT_SIZE_IN_BYTES), 2);
                unsafe.putFloat(buffer, normalize(arrayBaseOffset, Bits.FLOAT_SIZE_IN_BYTES),  3f);
                unsafe.putLong(buffer, normalize(arrayBaseOffset, Bits.LONG_SIZE_IN_BYTES), 4L);
                unsafe.putDouble(buffer, normalize(arrayBaseOffset, Bits.DOUBLE_SIZE_IN_BYTES), 5d);
                unsafe.copyMemory(new byte[buffer.length], arrayBaseOffset,
                        buffer, arrayBaseOffset,
                        buffer.length);

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

    static Unsafe findUnsafeIfAllowed() {
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

    static boolean isUnalignedAccessAllowed() {
        return AlignmentUtil.isUnalignedAccessAllowed();
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
