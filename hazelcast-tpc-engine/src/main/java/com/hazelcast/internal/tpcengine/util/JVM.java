/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
 * Various JVM utility functions.
 */
public final class JVM {

    private static final int MAJOR_VERSION = Runtime.version().feature();

    private JVM() {
    }

    /**
     * Gets the Major version of the JVM. So for e.g. Java '1.8' that would be '8' and for '17.1' that would be '17'.
     *
     * @return the major version.
     */
    public static int getMajorVersion() {
        return MAJOR_VERSION;
    }

    /**
     * Checks if the JVM is 32 bit.
     *
     * @return true if 32 bit.
     */
    public static boolean is32bit() {
        return !OS.is64bit();
    }
}
