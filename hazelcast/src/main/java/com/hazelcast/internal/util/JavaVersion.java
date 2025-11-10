/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
 * @see UnknownVersion
 * @see FutureJavaVersion
 */
public enum JavaVersion implements JavaMajorVersion {
    JAVA_18(18),
    JAVA_19(19),
    JAVA_20(20),
    JAVA_21(21)
    ;

    public static final JavaMajorVersion UNKNOWN_VERSION = new UnknownVersion();
    public static final JavaMajorVersion CURRENT_VERSION = detectCurrentVersion();

    private final int majorVersion;

    JavaVersion(int majorVersion) {
        this.majorVersion = majorVersion;
    }

    @Override
    public Integer getMajorVersion() {
        return majorVersion;
    }

    /**
     * Check if the current runtime version is greater than or equal to the given version.
     * Returns false if the given version is UNKNOWN.
     */
    public static boolean isAtLeast(JavaVersion version) {
        return isAtLeast(CURRENT_VERSION, version);
    }

    /**
     * Check if the current runtime version is less than or equal to the given version.
     * Returns false if the given version is UNKNOWN.
     */
    public static boolean isAtMost(JavaVersion version) {
        return CURRENT_VERSION != UNKNOWN_VERSION && version != UNKNOWN_VERSION
               && CURRENT_VERSION.getMajorVersion() <= version.getMajorVersion();
    }

    static boolean isAtLeast(JavaMajorVersion currentVersion, JavaMajorVersion minVersion) {
        return currentVersion != UNKNOWN_VERSION && minVersion != UNKNOWN_VERSION
                && currentVersion.getMajorVersion() >= minVersion.getMajorVersion();
    }

    /**
     * @see <a href="https://openjdk.org/jeps/223">
     *      JEP 223: New Version-String Scheme</a>
     */
    static JavaMajorVersion detectCurrentVersion() {
        ILogger logger = Logger.getLogger(JavaVersion.class);

        int major = JVM.getMajorVersion();

        if (logger.isFineEnabled()) {
            logger.fine("Detected runtime version: Java %s", major);
        }

        return Arrays.<JavaMajorVersion>stream(values())
                .filter(v -> v.getMajorVersion() == major).findFirst()
                .orElseGet(() -> new FutureJavaVersion(major));
    }

    /**
     * Represents an unknown Java version, which could not be parsed from system property
     * {@code java.version}. Comparisons involving unknown versions always return false.
     * This usually simplifies comparisons, but does not satisfy property:
     * {@code isAtLeast(v1, v2) == !isAtMost(v2, v1)}
     */
    static class UnknownVersion implements JavaMajorVersion {
        @Override
        public Integer getMajorVersion() {
            return null;
        }
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
        public Integer getMajorVersion() {
            return majorVersion;
        }
    }

    public static void main(String[] args) {
        System.out.println(CURRENT_VERSION.getMajorVersion());
    }
}

