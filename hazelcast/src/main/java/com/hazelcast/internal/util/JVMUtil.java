/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import com.hazelcast.internal.tpcengine.util.JVM;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;
import java.lang.management.ManagementFactory;

import static com.hazelcast.internal.memory.impl.UnsafeUtil.UNSAFE;
import static com.hazelcast.internal.memory.impl.UnsafeUtil.UNSAFE_AVAILABLE;
import static com.hazelcast.logging.Logger.getLogger;
import static java.lang.Math.abs;

/**
 * Helper class for retrieving JVM specific information.
 */
@SuppressWarnings("checkstyle:magicnumber")
public final class JVMUtil {

    /**
     * Defines the costs for a reference in Bytes.
     */
    public static final int REFERENCE_COST_IN_BYTES = JVM.is32bit() || isCompressedOops() ? 4 : 8;
    public static final int OBJECT_HEADER_SIZE;

    static {
        if (JVM.is32bit()) {
            OBJECT_HEADER_SIZE = 8;
        } else if (isCompactObjectHeader()) {
            // Compact Object header requires Compressed Class Pointers
            OBJECT_HEADER_SIZE = 8;
        } else if (isCompressedClassPointers()) {
            OBJECT_HEADER_SIZE = 12;
        } else {
            OBJECT_HEADER_SIZE = 16;
        }
    }

    private JVMUtil() {
    }

    // not private for testing
    public static boolean isCompressedOops() {
        // check HotSpot JVM implementation
        Boolean enabled = isHotSpotCompressedOopsOrNull();
        if (enabled != null) {
            return enabled;
        }

        // fallback check for other JVM implementations
        enabled = isObjectLayoutCompressedOopsOrNull();
        if (enabled != null) {
            return enabled;
        }

        // accept compressed oops is used by default
        getLogger(JVMUtil.class).info("Could not determine memory cost of reference; setting to default of 4 bytes.");
        return true;
    }

    // not private for testing
    public static boolean isCompressedClassPointers() {
        Boolean enabled = isHotSpotCompressedClassPointersOrNull();
        if (enabled != null) {
            return enabled;
        }

        // Assume compressed class pointers are enabled when CompressedOOPS are enabled.
        // This is not really true, but this fallback behaves in the same way as previous logic.
        getLogger(JVMUtil.class).fine("Could not determine if compressed class pointers are enabled;"
                + " setting same as compressed OOPS.");
        return isCompressedOops();
    }

    // not private for testing
    public static boolean isCompactObjectHeader() {
        Boolean enabled = isHotSpotCompactObjectHeaderOrNull();
        if (enabled != null) {
            return enabled;
        }

        // assume compact object headers are not used by default
        // (to be revised in the future, as of JDK25 it is a reasonable assumption)
        getLogger(JVMUtil.class).fine("Could not determine if compact object headers are enabled; setting to default disabled.");
        return false;
    }

    /**
     * @return the process ID
     * @see ProcessHandle#pid()
     */
    public static long getPid() {
        return ProcessHandle.current().pid();
    }

    /**
     * Returns used memory as reported by the {@link Runtime} and the function (totalMemory - freeMemory)
     * It attempts to correct atomicity issues (i.e. when totalMemory expands) and reported usedMemory
     * results in negative values
     *
     * @return an approximation to the total amount of memory currently
     * used, measured in bytes.
     */
    public static long usedMemory(final Runtime runtime) {
        long totalBegin;
        long totalEnd;
        long used;
        do {
            totalBegin = runtime.totalMemory();
            used = totalBegin - runtime.freeMemory();
            totalEnd = runtime.totalMemory();
        } while (totalBegin != totalEnd);

        return used;
    }

    // not private for testing
    static Boolean isHotSpotCompressedOopsOrNull() {
        return getHotSpotOption("UseCompressedOops");
    }

    static Boolean isHotSpotCompressedClassPointersOrNull() {
        // note: UseCompressedClassPointers is deprecated in JDK25 and removed in JDK27
        return getHotSpotOption("UseCompressedClassPointers");
    }


    /**
     * Fallback when checking CompressedOopsEnabled.
     */
    static Boolean isObjectLayoutCompressedOopsOrNull() {
        if (!UNSAFE_AVAILABLE) {
            return null;
        }

        Integer referenceSize = ReferenceSizeEstimator.getReferenceSizeOrNull();
        if (referenceSize == null) {
            return null;
        }

        // when reference size does not equal address size then it's safe to assume references are compressed
        return referenceSize != UNSAFE.addressSize();
    }

    // not private for testing
    static Boolean isHotSpotCompactObjectHeaderOrNull() {
        return getHotSpotOption("UseCompactObjectHeaders");
    }

    private static Boolean getHotSpotOption(String propertyName) {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            ObjectName mbean = new ObjectName("com.sun.management:type=HotSpotDiagnostic");
            Object[] objects = {propertyName};
            String[] strings = {"java.lang.String"};
            String operation = "getVMOption";
            CompositeDataSupport compressedOopsValue = (CompositeDataSupport) server.invoke(mbean, operation, objects, strings);
            return Boolean.valueOf(compressedOopsValue.get("value").toString());
        } catch (Exception e) {
            getLogger(JVMUtil.class).fine("Failed to read HotSpot specific configuration: " + e.getMessage());
        }
        return null;
    }

    /**
     * Estimates the reference by comparing the address offset of two fields.
     * <p>
     * We can't rely on Unsafe to get a real reference size when oops compression is enabled.
     * Hence, we have to do a simple experiment: Let's have a class with 2 references.
     * The difference between address offsets is the reference size in bytes.
     * <p>
     * It is not bullet-proof, it assumes a certain object layout, but this happens
     * to work for all JVMs tested.
     */
    @SuppressWarnings({"unused", "checkstyle:visibilitymodifier"})
    private static final class ReferenceSizeEstimator {

        public Object firstField;
        public Object secondField;

        static Integer getReferenceSizeOrNull() {
            Integer referenceSize = null;
            try {
                long firstFieldOffset = UNSAFE.objectFieldOffset(ReferenceSizeEstimator.class.getField("firstField"));
                long secondFieldOffset = UNSAFE.objectFieldOffset(ReferenceSizeEstimator.class.getField("secondField"));
                referenceSize = (int) abs(secondFieldOffset - firstFieldOffset);
            } catch (Exception e) {
                getLogger(JVMUtil.class).fine("Could not determine cost of reference using field offsets: " + e.getMessage());
            }
            return referenceSize;
        }
    }
}
