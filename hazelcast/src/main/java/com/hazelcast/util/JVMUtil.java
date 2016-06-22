/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;
import java.lang.management.ManagementFactory;

import static com.hazelcast.logging.Logger.getLogger;
import static com.hazelcast.nio.UnsafeHelper.UNSAFE;
import static com.hazelcast.nio.UnsafeHelper.UNSAFE_AVAILABLE;

/**
 * Helper class for retrieving JVM specific information.
 */
public final class JVMUtil {

    /**
     * Defines the costs for a reference in Bytes.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int REFERENCE_COST_IN_BYTES = isCompressedOops() ? 4 : 8;

    private JVMUtil() {
    }

    private static boolean isCompressedOops() {
        // check HotSpot JVM implementation
        Boolean enabled = isHotSpotCompressedOopsOrNull();
        if (enabled != null) {
            return enabled;
        }

        // fallback check for other JVM implementations
        enabled = isCompressedOopsOrNull();
        if (enabled != null) {
            return enabled;
        }

        // accept compressed oops is used by default
        return true;
    }

    @SuppressFBWarnings("NP_BOOLEAN_RETURN_NULL")
    private static Boolean isHotSpotCompressedOopsOrNull() {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            ObjectName mbean = new ObjectName("com.sun.management:type=HotSpotDiagnostic");
            Object[] objects = {"UseCompressedOops"};
            String[] strings = {"java.lang.String"};
            String operation = "getVMOption";
            CompositeDataSupport compressedOopsValue = (CompositeDataSupport) server.invoke(mbean, operation, objects, strings);
            return Boolean.valueOf(compressedOopsValue.get("value").toString());
        } catch (Exception e) {
            getLogger(JVMUtil.class).warning("Failed to read HotSpot specific configuration", e);
        }

        return null;
    }

    /**
     * Fallback when checking CompressedOopsEnabled.
     *
     * Borrowed from http://openjdk.java.net/projects/code-tools/jol/
     */
    @SuppressFBWarnings("NP_BOOLEAN_RETURN_NULL")
    private static Boolean isCompressedOopsOrNull() {
        if (!UNSAFE_AVAILABLE) {
            return null;
        }

        // when running with CompressedOops on 64-bit platform, the address size reported by Unsafe is still 8, while
        // the real reference fields are 4 bytes long, so we try to guess the reference field size with this naive trick
        int oopSize;
        try {
            long off1 = UNSAFE.objectFieldOffset(CompressedOopsClass.class.getField("obj1"));
            long off2 = UNSAFE.objectFieldOffset(CompressedOopsClass.class.getField("obj2"));
            oopSize = (int) Math.abs(off2 - off1);
        } catch (Exception e) {
            getLogger(JVMUtil.class).warning(e);
            return null;
        }

        return oopSize != UNSAFE.addressSize();
    }

    @SuppressWarnings("unused")
    private static class CompressedOopsClass {

        Object obj1;
        Object obj2;
    }
}
