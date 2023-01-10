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

package com.hazelcast.internal.tpc.util;

import java.lang.reflect.Method;

import static java.lang.Runtime.getRuntime;

/**
 * Various JVM utility functions.
 */
public class JVM {

    private final static int MAJOR_VERSION = getMajorVersion0();

    /**
     * Gets the Major version of the JVM. So for e.g. Java '1.8' that would be '8' and for '17.1' that would be '17'.
     *
     * @return the major version.
     */
    public static int getMajorVersion() {
        return MAJOR_VERSION;
    }

    private static int getMajorVersion0() {
        // First try the Runtime version (Java 9+)
        try {
            Method versionMethod = Runtime.class.getDeclaredMethod("version");
            if (versionMethod != null) {
                Class<?> versionClazz = Class.forName("java.lang.Runtime$Version");
                Method majorMethod = versionClazz.getDeclaredMethod("major");
                Object versionObject = versionMethod.invoke(getRuntime());
                return (Integer) majorMethod.invoke(versionObject);
            }
        } catch (Exception e) {
            // fall back to parsing the java.version system property
        }

        return parseVersionString(System.getProperty("java.version"));
    }

    static int parseVersionString(String javaVersion) {
        String majorVersion;
        if (javaVersion.startsWith("1.")) {
            majorVersion = javaVersion.substring(2, 3);
        } else {
            int dotIndex = javaVersion.indexOf(".");
            if (dotIndex == -1) {
                majorVersion = javaVersion;
            } else {
                majorVersion = javaVersion.substring(0, dotIndex);
            }
        }
        return Integer.parseInt(majorVersion);
    }

    private JVM() {
    }
}
