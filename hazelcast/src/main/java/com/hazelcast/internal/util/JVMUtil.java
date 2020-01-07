/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
public final class JVMUtil {

    /**
     * Defines the costs for a reference in Bytes.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int REFERENCE_COST_IN_BYTES = is32bitJVM() || isCompressedOops() ? 4 : 8;

    private JVMUtil() {
    }

    public static boolean is32bitJVM() {
        // sun.arch.data.model is available on Oracle, Zing and (most probably) IBM JVMs
        String architecture = System.getProperty("sun.arch.data.model");
        return architecture != null && architecture.equals("32");
    }

    // not private for testing
    static boolean isCompressedOops() {
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

    /**
     * Returns used memory as reported by the {@link Runtime} and the function (totalMemory - freeMemory)
     * It attempts to correct atomicity issues (ie. when totalMemory expands) and reported usedMemory
     * results in negative values
     *
     * @param runtime
     * @return an approximation to the total amount of memory currently
 *             used, measured in bytes.
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
    @SuppressFBWarnings("NP_BOOLEAN_RETURN_NULL")
    static Boolean isHotSpotCompressedOopsOrNull() {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            ObjectName mbean = new ObjectName("com.sun.management:type=HotSpotDiagnostic");
            Object[] objects = {"UseCompressedOops"};
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
     * Fallback when checking CompressedOopsEnabled.
     */
    @SuppressFBWarnings("NP_BOOLEAN_RETURN_NULL")
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

    /**
     * Estimates the reference by comparing the address offset of two fields.
     *
     * We can't rely on Unsafe to get a real reference size when oops compression is enabled.
     * Hence we have to do a simple experiment: Let's have a class with 2 references.
     * The difference between address offsets is the reference size in bytes.
     *
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
