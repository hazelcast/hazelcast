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

package com.hazelcast.internal.tpcengine.util;

/**
 * Utility methods for OS specific functionality.
 */
@SuppressWarnings("checkstyle:MethodName")
// https://lopica.sourceforge.net/os.html
// https://memorynotfound.com/detect-os-name-version-java/
public final class OS {
    private static final String OS_NAME = System.getProperty("os.name", "?");
    private static final String OS_VERSION = System.getProperty("os.version", "?");
    private static final boolean IS_LINUX = isLinux0(OS_NAME);
    private static final boolean IS_UNIX_FAMILY = isUnixFamily0(OS_NAME);
    private static final boolean IS_WINDOWS = isWindows0(OS_NAME);
    private static final boolean IS_MAC = isMac0(OS_NAME);

    private static final int LINUX_KERNEL_MAJOR_VERSION = linuxMajorVersion0(OS_VERSION, IS_LINUX);
    private static final int LINUX_KERNEL_MINOR_VERSION = linuxMinorVersion0(OS_VERSION, IS_LINUX);
    private static final int PAGE_SIZE = UnsafeLocator.UNSAFE.pageSize();
    private static final String OS_ARCH = System.getProperty("os.arch", "?");
    private static final boolean IS_64BIT = is64bit0(OS_ARCH);
    private static final boolean IS_X86_64 = OS_ARCH.equals("amd64");

    private OS() {
    }

    private static boolean is64bit0(String osArch) {
        return osArch.contains("64");
    }

    static boolean isLinux0(String osName) {
        return osName.toLowerCase().startsWith("linux");
    }

    static boolean isUnixFamily0(String osName) {
        osName = osName.toLowerCase();
        return IS_LINUX || osName.contains("nix") || osName.contains("aix");
    }

    static boolean isWindows0(String osName) {
        return osName.toLowerCase().contains("windows");
    }

    static boolean isMac0(String osName) {
        osName = osName.toLowerCase();
        return (osName.contains("mac") || osName.contains("darwin"));
    }

    static int linuxMajorVersion0(String version, boolean isLinux) {
        if (!isLinux) {
            return -1;
        }

        String[] versionTokens = version.split("\\.");
        try {
            return Integer.parseInt(versionTokens[0]);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    static int linuxMinorVersion0(String version, boolean isLinux) {
        if (!isLinux) {
            return -1;
        }

        String[] versionTokens = version.split("\\.");
        try {
            return Integer.parseInt(versionTokens[1]);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /** @return true if the architecture of the os is x86-64 (AMD64), false otherwise. */
    @SuppressWarnings("java:S100")
    public static boolean isX86_64() {
        return IS_X86_64;
    }

    /** @return the page size (so the size of a single page in the page table). */
    public static int pageSize() {
        return PAGE_SIZE;
    }

    /** @return {@code true} if the current system from Unix family (Unix/Linux/AIX). */
    public static boolean isUnixFamily() {
        return IS_UNIX_FAMILY;
    }

    /** @return {@code true} if the current system is Linux. */
    public static boolean isLinux() {
        return IS_LINUX;
    }

    /** @return the OS name ("os.name" System property). */
    public static String osName() {
        return OS_NAME;
    }

    /** @return the OS version ("os.version" System property). */
    public static String osVersion() {
        return OS_VERSION;
    }

    /** @return the OS arch ("os.arch" System property). */
    public static String osArch() {
        return OS_ARCH;
    }

    /**
     * @return the Linux kernel major version or -1 if it couldn't be determined.
     * @throws IllegalStateException when the OS isn't Linux.
     */
    public static int linuxKernelMajorVersion() {
        if (!IS_LINUX) {
            throw new IllegalStateException(OS_NAME + " is not a Linux OS");
        }

        return LINUX_KERNEL_MAJOR_VERSION;
    }

    /**
     * @return the Linux kernel minor version or -1 if it couldn't be determined.
     * @throws IllegalStateException when the OS isn't Linux.
     */
    public static int linuxKernelMinorVersion() {
        if (!IS_LINUX) {
            throw new IllegalStateException(OS_NAME + " is not a Linux OS");
        }

        return LINUX_KERNEL_MINOR_VERSION;
    }

    /** @return true if the OS 64 bit, false otherwise. */
    public static boolean is64bit() {
        return IS_64BIT;
    }

    /** @return {@code true} if the current system is Mac OS. */
    public static boolean isMac() {
        return IS_MAC;
    }

    /** @return {@code true} if the current system is a Windows one. */
    public static boolean isWindows() {
        return IS_WINDOWS;
    }
}
