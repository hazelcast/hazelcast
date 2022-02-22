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

import com.hazelcast.internal.util.collection.ArrayUtils;

import java.nio.ByteOrder;

import static com.hazelcast.internal.memory.impl.UnsafeUtil.UNSAFE_AVAILABLE;

@SuppressWarnings("checkstyle:magicnumber")
public final class AlignmentUtil {

    public static final int OBJECT_REFERENCE_ALIGN = UNSAFE_AVAILABLE
            ? UnsafeUtil.UNSAFE.arrayIndexScale(Object[].class)
            : -1;

    public static final int OBJECT_REFERENCE_MASK = OBJECT_REFERENCE_ALIGN - 1;
    public static final boolean IS_PLATFORM_BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    // We know these architectures allow unaligned memory access.
    // There is a chance some other architectures allow it too, but it's
    // better to stay conservative here.
    // For example unaligned memory access on Sparc may crash JVM with SIGBUS
    private static final String[] ARCHITECTURES_KNOWN_TO_ALLOW_UNALIGNED_ACCESS = new String[]
            {"i386", "x86", "amd64", "x86_64"};

    private AlignmentUtil() {
    }

    public static boolean is2BytesAligned(long address) {
        return (address & 0x01) == 0;
    }

    public static boolean is4BytesAligned(long address) {
        return (address & 0x03) == 0;
    }

    public static boolean is8BytesAligned(long address) {
        return (address & 0x07) == 0;
    }

    public static boolean isReferenceAligned(long address) {
        return (address & OBJECT_REFERENCE_MASK) == 0;
    }

    public static void checkReferenceAligned(long address) {
        if (!isReferenceAligned(address)) {
            throw new IllegalArgumentException("Memory access to object references must be "
                    + OBJECT_REFERENCE_ALIGN + "-bytes aligned, but the address used was " + address);
        }
    }

    public static void check2BytesAligned(long address) {
        if (!is2BytesAligned(address)) {
            throw new IllegalArgumentException("Atomic memory access must be aligned, but the address used was " + address);
        }
    }

    public static void check4BytesAligned(long address) {
        if (!is4BytesAligned(address)) {
            throw new IllegalArgumentException("Atomic memory access must be aligned, but the address used was " + address);
        }
    }

    public static void check8BytesAligned(long address) {
        if (!is8BytesAligned(address)) {
            throw new IllegalArgumentException("Atomic memory access must be aligned, but the address used was " + address);
        }
    }

    public static boolean isUnalignedAccessAllowed() {
        String currentArchitecture = System.getProperty("os.arch");
        return ArrayUtils.contains(ARCHITECTURES_KNOWN_TO_ALLOW_UNALIGNED_ACCESS, currentArchitecture);
    }
}
