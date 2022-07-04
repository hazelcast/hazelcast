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

package com.hazelcast.internal.util;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import net.openhft.affinity.Affinity;

import java.io.File;
import java.io.InputStream;
import java.util.BitSet;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.copy;
import static com.hazelcast.internal.nio.IOUtil.getFileFromResourcesAsStream;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.StringUtil.equalsIgnoreCase;

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
public final class ThreadAffinityHelper {
    private static final String AFFINITY_LIB_DISABLED = "hazelcast.affinity.lib.disabled";
    private static final ILogger LOGGER = Logger.getLogger(ThreadAffinityHelper.class);
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
                Logger.getLogger(ThreadAffinityHelper.class).warning("JNA is not available");
            }
            return jnaAvailable;
        } catch (NoClassDefFoundError e) {
            Logger.getLogger(ThreadAffinityHelper.class).warning("The OpenHFT Affinity jar isn't available: " + e.getMessage());
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

    private static String extractBundledLib() {
        InputStream src = null;
        try {
            src = getFileFromResourcesAsStream("lib/linux-x86_64/libaffinity_helper.so");
            File dest = File.createTempFile("hazelcast-libaffinity-helper-", ".so");

            copy(src, dest);

            return dest.getAbsolutePath();
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            closeResource(src);
        }
    }

    static {
        boolean hzAffinityLibLoaded = false;
        boolean libDisabled = equalsIgnoreCase("true", System.getProperty(AFFINITY_LIB_DISABLED));
        if (!libDisabled && OsHelper.isLinux() && !JVMUtil.is32bitJVM()) {
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
