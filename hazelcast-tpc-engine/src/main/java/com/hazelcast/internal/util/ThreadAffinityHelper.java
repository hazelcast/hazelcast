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

package com.hazelcast.internal.util;

import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;
import com.hazelcast.internal.tpcengine.util.CloseUtil;
import com.hazelcast.internal.tpcengine.util.JVM;
import com.hazelcast.internal.tpcengine.util.OS;
import net.openhft.affinity.Affinity;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.BitSet;

import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;

/**
 * Affinity helper class that uses a Hazelcast developed affinity
 * shared library on Linux x86_64 systems. On other systems this class
 * falls back to using OpenHFT affinity. The aim of this class is to ease
 * up configuring CPU affinity on the Linux x86_64 systems, because the
 * OpenHFT library requires the users to load a shared library and adding
 * the OpenHFT jar to the classpath.
 * <p/>
 * This class uses a small shared library bundled into the Hazelcast
 * open-source and enterprise edition jars.
 * <p/>
 * If the {@code hazelcast.affinity.lib.disabled} system property is set
 * to {@code true}, there is no attempt made for using the Hazelcast
 * library and the OpenHFT library is used instead in every case.
 */
@SuppressWarnings("java:S1181")
public final class ThreadAffinityHelper {
    private static final String AFFINITY_LIB_DISABLED = "hazelcast.affinity.lib.disabled";
    private static final TpcLogger LOGGER = TpcLoggerLocator.getLogger(ThreadAffinityHelper.class);
    private static final boolean USE_HZ_LIB;

    private ThreadAffinityHelper() {
    }

    static boolean isAffinityAvailable() {
        if (USE_HZ_LIB) {
            return true;
        }

        // check if the OpenHFT Affinity library is available
        try {
            boolean jnaAvailable = Affinity.isJNAAvailable();
            if (!jnaAvailable) {
                LOGGER.warning("JNA is not available");
            }
            return jnaAvailable;
        } catch (NoClassDefFoundError e) {
            LOGGER.warning("The OpenHFT Affinity jar isn't available: " + e.getMessage());
            return false;
        }
    }

    public static BitSet getAffinity() {
        try {
            if (USE_HZ_LIB) {
                return getAffinity0();
            } else {
                return Affinity.getAffinity();
            }
        } catch (Exception ex) {
            LOGGER.warning("An error occurred while getting the affinity for the current thread", ex);
            return new BitSet();
        }
    }

    public static void setAffinity(BitSet cpuMask) {
        try {
            if (USE_HZ_LIB) {
                setAffinity0(cpuMask);
            } else {
                Affinity.setAffinity(cpuMask);
            }
        } catch (Exception ex) {
            LOGGER.warning("An error occurred while setting the affinity for the current thread", ex);
        }
    }

    private static native BitSet getAffinity0();

    private static native void setAffinity0(BitSet cpuMask);

    @SuppressWarnings({"java:S5443", "java:S112"})
    private static String extractBundledLib() {
        InputStream src = null;
        try {
            src = CloseUtil.class.getClassLoader().getResourceAsStream("lib/linux-x86_64/libaffinity_helper.so");
            File dest = File.createTempFile("hazelcast-libaffinity-helper-", ".so");
            Files.copy(src, dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return dest.getAbsolutePath();

        } catch (Error | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeQuietly(src);
        }
    }

    static {
        boolean hzAffinityLibLoaded = false;

        boolean libDisabled = false;
        String libDisabledString = System.getProperty(AFFINITY_LIB_DISABLED);
        if (libDisabledString != null) {
            libDisabled = libDisabledString.equalsIgnoreCase("true");
        }

        if (!libDisabled && OS.isLinux() && !JVM.is32bit()) {
            try {
                System.load(extractBundledLib());
                hzAffinityLibLoaded = true;
            } catch (Throwable t) {
                LOGGER.fine("Could not load Hazelcast Linux x86_64 CPU affinity helper native shared library", t);
            }
        }

        USE_HZ_LIB = hzAffinityLibLoaded;
    }
}
