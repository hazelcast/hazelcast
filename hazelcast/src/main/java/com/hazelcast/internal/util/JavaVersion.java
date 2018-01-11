/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
 * Utility for checking runtime Java version.
 *
 */
public enum JavaVersion {
    UNKNOWN,
    JAVA_1_6,
    JAVA_1_7,
    JAVA_1_8,
    JAVA_1_9;

    private static final JavaVersion CURRENT_VERSION = detectCurrentVersion();

    /**
     * Check if the current runtime version is at least the given version.
     *
     * @param version version to be compared against the current runtime version
     * @return Return true if current runtime version of Java is the same or greater than given version.
     *         When the passed version is {@link #UNKNOWN} then it always returns true.
     */
    public static boolean isAtLeast(JavaVersion version) {
        return isAtLeast(CURRENT_VERSION, version);
    }

    private static JavaVersion detectCurrentVersion() {
        String version = System.getProperty("java.version");
        return parseVersion(version);
    }

    static JavaVersion parseVersion(String version) {
        if (version == null) {
            // this should not happen but it's better to stay on the safe side
            return UNKNOWN;
        }
        if (version.startsWith("1.")) {
            String withoutMajor = version.substring(2, version.length());
            if (withoutMajor.startsWith("6")) {
                return JAVA_1_6;
            } else if (withoutMajor.startsWith("7")) {
                return JAVA_1_7;
            } else if (withoutMajor.startsWith("8")) {
                return JAVA_1_8;
            }
            return UNKNOWN;
        } else if (version.startsWith("9")) {
            // from version 9 the string does not start with "1."
            return JAVA_1_9;
        }
        return UNKNOWN;
    }

    static boolean isAtLeast(JavaVersion currentVersion, JavaVersion minVersion) {
        return currentVersion.ordinal() >= minVersion.ordinal();
    }
}
