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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Helper methods related to operating system on which the code is actually running.
 */
public final class OsHelper {

    /**
     * OS name in lower case.
     */
    public static final String OS = StringUtil.lowerCaseInternal(System.getProperty("os.name"));

    private OsHelper() {
    }

    /**
     * Returns {@code true} if the system is Linux.
     *
     * @return {@code true} if the current system is Linux.
     */
    public static boolean isLinux() {
        return OS.contains("nux");
    }

    /**
     * Returns {@code true} if the system is from Unix family.
     *
     * @return {@code true} if the current system is Unix/Linux/AIX.
     */
    public static boolean isUnixFamily() {
        return (OS.contains("nix") || OS.contains("nux") || OS.contains("aix"));
    }

    /**
     * Returns {@code true} if the system is a Mac OS.
     *
     * @return {@code true} if the current system is Mac.
     */
    public static boolean isMac() {
        return (OS.contains("mac") || OS.contains("darwin"));
    }

    /**
     * Returns {@code true} if the system is a Mac OS with Arm CPU, e.g. M1.
     *
     * @return {@code true} if the system is a Mac OS with Arm CPU, e.g. M1.
     */
    public static boolean isArmMac() {
        if (!isMac()) {
            return false;
        }
        try {
            Process process = new ProcessBuilder("sysctl", "-n", "hw.optional.arm64")
                    .redirectErrorStream(true)
                    .start();
            process.waitFor();
            try (InputStream is = process.getInputStream()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                String line = reader.readLine();
                return line.equals("1");
            }
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Returns {@code true} if the system is a Windows.
     *
     * @return {@code true} if the current system is a Windows one.
     */
    public static boolean isWindows() {
        return OS.contains("windows");
    }
}
