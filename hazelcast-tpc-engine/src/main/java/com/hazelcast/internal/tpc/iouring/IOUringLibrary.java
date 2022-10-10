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

package com.hazelcast.internal.tpc.iouring;

import com.hazelcast.internal.tpc.util.JVM;
import com.hazelcast.internal.tpc.util.OS;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.UUID;

import static com.hazelcast.internal.tpc.util.OS.linuxKernelMajorVersion;
import static com.hazelcast.internal.tpc.util.OS.linuxKernelMinorVersion;
import static com.hazelcast.internal.tpc.util.OS.osName;
import static com.hazelcast.internal.tpc.util.OS.osVersion;

/**
 * Contains the functionality for loading the IOUring library.
 */
public final class IOUringLibrary {
    private static final int MIN_MAJOR_VERSION = 5;
    // We need at least 5.6 due to reliance on IORING_OP_TIMEOUT
    private static final int MIN_MINOR_VERSION = 6;

    private final static Throwable LOAD_FAILURE;

    private IOUringLibrary() {
    }

    static {
        Throwable cause = null;
        try {
            if (!OS.isLinux()) {
                throw new UnsupportedOperationException("Linux is required, but found OS [" + osName() + "]");
            }

            if (!OS.is64bit()) {
                throw new UnsupportedOperationException("Linux 64 bit is required, but Linux 32 bit is detected.");
            }

            if (!isValidLinuxKernel()) {
                throw new UnsupportedOperationException("Linux >= " + MIN_MAJOR_VERSION + "." + MIN_MINOR_VERSION
                        + " is required, but found version [" + osVersion() + "]");
            }

            if (!OS.isX86_64()) {
                throw new UnsupportedOperationException("x86-64 is required, but found " + OS.osArch());
            }

            if (!JVM.is64bit()) {
                throw new UnsupportedOperationException("64 bit JVM is required, but detected 32 bit.");
            }

            String path = "/lib/linux-x86_64/libiouring_hz.so";
            URL url = IOUring.class.getResource(path);
            if (url == null) {
                throw new IOException("Could not find [" + path + "] in the hazelcast-tpc-engine jar");
            }

            String tmpDir = System.getProperty("java.io.tmpdir");
            File destination = new File(tmpDir, "libiouring_hz-" + UUID.randomUUID() + ".so");
            destination.deleteOnExit();
            Files.copy(url.openStream(), destination.toPath());
            System.load(destination.getAbsolutePath());
        } catch (Throwable e) {
            cause = e;
        }

        LOAD_FAILURE = cause;
    }

    /**
     * Checks if Hazelcast io_uring library is available.
     *
     * @return true if available, false otherwise.
     */
    public static boolean isAvailable() {
        return LOAD_FAILURE == null;
    }

    /**
     * Ensures that the Hazelcast io_uring library is available.
     *
     * @throws Error if the library isn't available.
     */
    public static void ensureAvailable() {
        if (LOAD_FAILURE != null) {
            throw new Error("Failed to load the Hazelcast io_uring library", LOAD_FAILURE);
        }
    }

    private static boolean isValidLinuxKernel() {
        if (linuxKernelMajorVersion() < MIN_MAJOR_VERSION) {
            return false;
        } else if (linuxKernelMajorVersion() == MIN_MAJOR_VERSION && linuxKernelMinorVersion() < MIN_MINOR_VERSION) {
            return false;
        } else {
            return true;
        }
    }

}
