/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
     * Returns {@code true} if the system is a Windows.
     *
     * @return {@code true} if the current system is a Windows one.
     */
    public static boolean isWindows() {
        return OS.contains("windows");
    }
}
