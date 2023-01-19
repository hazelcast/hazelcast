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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Arrays;

/**
 * Utility for checking runtime Java version.
 * <p>
 * Since we rely on a public Java API returning the major Java version,
 * version comparisons can be done safely even with versions that didn't
 * exist at the time the given Hazelcast version was released.
 *
 * @see FutureJavaVersion
 */
public enum JavaVersion implements JavaMajorVersion {
    JAVA_8(8),
    JAVA_9(9),
    JAVA_10(10),
    JAVA_11(11),
    JAVA_12(12),
    JAVA_13(13),
    JAVA_14(14),
    JAVA_15(15),
    JAVA_16(16),
    JAVA_17(17),
    JAVA_18(18),
    JAVA_19(19),
    JAVA_20(20)
    ;

    public static final JavaMajorVersion CURRENT_VERSION = detectCurrentVersion();

    private final int majorVersion;

    JavaVersion(int majorVersion) {
        this.majorVersion = majorVersion;
    }

    @Override
    public int getMajorVersion() {
        return majorVersion;
    }

    /**
     * Check if the current runtime version is greater than or equal to the given version.
     */
    public static boolean isAtLeast(JavaVersion version) {
        return isAtLeast(CURRENT_VERSION, version);
    }

    /**
     * Check if the current runtime version is less than or equal to the given version.
     */
    public static boolean isAtMost(JavaVersion version) {
        return isAtMost(CURRENT_VERSION, version);
    }

    static boolean isAtLeast(JavaMajorVersion currentVersion, JavaMajorVersion minVersion) {
        return currentVersion.getMajorVersion() >= minVersion.getMajorVersion();
    }

    static boolean isAtMost(JavaMajorVersion currentVersion, JavaMajorVersion maxVersion) {
        return currentVersion.getMajorVersion() <= maxVersion.getMajorVersion();
    }

    /**
     * <pre>
     * <= JDK 8
     *     VNUM  := 1\.[1-9]\.[0-9](_[0-9]{2})?         Version number
     *     PRE   := [a-zA-Z0-9]+                        Pre-release identifier
     *     java.version
     *           := $VNUM(-$PRE)*
     * >= JDK 9
     *     VNUM  := [1-9][0-9]*(\.(0|[1-9][0-9]*))*     Version number
     *     PRE   := [a-zA-Z0-9]+                        Pre-release identifier
     *     BUILD := 0|[1-9][0-9]*                       Build number
     *     OPT   := [-a-zA-Z0-9\.]+                     Build information
     *     java.version
     *           := $VNUM(-$PRE)?\+$BUILD(-$OPT)?
     *            | $VNUM-$PRE(-$OPT)?
     *            | $VNUM(+-$OPT)?
     * </pre>
     * @see <a href="https://www.oracle.com/java/technologies/javase/versioning-naming.html">
     *      J2SE SDK/JRE Version String Naming Convention (<= JDK 8)
     * @see <a href="https://openjdk.org/jeps/223">
     *      JEP 223: New Version-String Scheme (>= JDK 9)
     */
    private static JavaMajorVersion detectCurrentVersion() {
        String[] version = System.getProperty("java.version").split("[-+.]");
        int major = Integer.parseInt(version[version[0].equals("1") ? 1 : 0]);

        ILogger logger = Logger.getLogger(JavaVersion.class);
        if (logger.isFineEnabled()) {
            logger.fine("Detected runtime version: Java " + major);
        }

        return Arrays.<JavaMajorVersion>stream(values())
                .filter(v -> v.getMajorVersion() == major).findFirst()
                .orElseGet(() -> new FutureJavaVersion(major));
    }

    /**
     * Represents a future Java version that has not yet added to the {@link JavaVersion}
     * enum. This class allows comparison of known versions against future, not yet
     * listed (in the enum) Java versions, making {@link #isAtLeast(JavaVersion)} and
     * {@link #isAtMost(JavaVersion)} methods usable in this case too.
     */
    static class FutureJavaVersion implements JavaMajorVersion {
        private final int majorVersion;

        FutureJavaVersion(int majorVersion) {
            this.majorVersion = majorVersion;
        }

        @Override
        public int getMajorVersion() {
            return majorVersion;
        }
    }

    public static void main(String[] args) {
        System.out.println(CURRENT_VERSION.getMajorVersion());
    }
}

