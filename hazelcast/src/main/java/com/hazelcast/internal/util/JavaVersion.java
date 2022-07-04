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

import java.lang.reflect.Method;

/**
 * Utility for checking runtime Java version.
 * <p>
 * This class relies on the {@code java.lang.Runtime.Version.major()} method
 * available since Java 9, therefore it's accessed via reflection. If
 * {@code java.lang.Runtime.Version} is not present, we treat the runtime
 * environment as Java 8. Also, if there is any exception thrown during the
 * reflective access, we fall back to Java 8.
 * <p>
 * Since we rely on a public Java API returning the major Java version
 * version comparisons can be done safely even with versions that didn't
 * exist at the time the given Hazelcast version was released.
 * See {@link FutureJavaVersion).
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
    JAVA_17(17)
    ;

    public static final JavaMajorVersion CURRENT_VERSION = detectCurrentVersion();

    private int majorVersion;

    JavaVersion(int majorVersion) {
        this.majorVersion = majorVersion;
    }

    @Override
    public int getMajorVersion() {
        return majorVersion;
    }

    /**
     * Check if the current runtime version is at least the given version.
     *
     * @param version version to be compared against the current runtime version
     * @return Return true if current runtime version of Java is the same or greater than given version.
     */
    public static boolean isAtLeast(JavaVersion version) {
        return isAtLeast(CURRENT_VERSION, version);
    }

    /**
     * Check if the current runtime version is at most the given version.
     *
     * @param version version to be compared against the current runtime version
     * @return Return true if current runtime version of Java is the same or less than given version.
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

    private static JavaVersion valueOf(int majorVersion) {
        for (JavaVersion version : values()) {
            if (version.majorVersion == majorVersion) {
                return version;
            }
        }

        return null;
    }

    private static JavaMajorVersion detectCurrentVersion() {
        final ILogger logger = Logger.getLogger(JavaVersion.class);
        final Class runtimeClass = Runtime.class;
        final Class versionClass;

        try {
            versionClass = Class.forName("java.lang.Runtime$Version");
        } catch (ClassNotFoundException e) {
            // if Runtime is present but Runtime.Version doesn't, it's Java8 for sure
            if (logger.isFineEnabled()) {
                logger.fine("Detected runtime version: Java 8");
            }
            return JAVA_8;
        }

        try {
            Method versionMethod = runtimeClass.getDeclaredMethod("version");
            Object versionObj = versionMethod.invoke(Runtime.getRuntime());
            Method majorMethod = versionClass.getDeclaredMethod("major");
            int majorVersion = (int) majorMethod.invoke(versionObj);

            if (logger.isFineEnabled()) {
                logger.fine("Detected runtime version: Java " + majorVersion);
            }

            JavaVersion foundVersion = valueOf(majorVersion);
            if (foundVersion != null) {
                return foundVersion;
            }

            return new FutureJavaVersion(majorVersion);
        } catch (Exception e) {
            logger.warning("Unable to detect Java version, falling back to Java 8", e);
            return JAVA_8;
        }
    }

    /**
     * Represents a future Java version that has not yet added to the
     * {@link JavaVersion} enum. This class allows comparison of known
     * versions against future, not yet listed (in the enum) Java versions,
     * making {@link JavaVersion#isAtLeast(JavaVersion)} and
     * {@link JavaVersion#isAtMost(JavaVersion)} methods usable in this
     * case too.
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

