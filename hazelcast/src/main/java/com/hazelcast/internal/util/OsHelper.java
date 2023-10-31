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

import com.hazelcast.internal.tpcengine.util.OS;

/**
 * Helper methods related to operating system on which the code is actually running.
 *
 * @deprecated use {@link OS}
 */
@Deprecated(since = "5.4")
public final class OsHelper {
    private OsHelper() {
    }

    /**
     * Returns {@code true} if the system is from Unix family.
     *
     * @deprecated call {@link OS#isLinux()} directly
     * @return {@code true} if the current system is Unix/Linux/AIX.
     */
    @Deprecated(since = "5.4")
    public static boolean isLinux() {
        return OS.isLinux();
    }

    /**
     * Returns {@code true} if the system is from Unix family.
     *
     * @deprecated call {@link OS#isUnixFamily()} directly
     * @return {@code true} if the current system is Unix/Linux/AIX.
     */
    @Deprecated(since = "5.4")
    public static boolean isUnixFamily() {
        return OS.isUnixFamily();
    }

    /**
     * Returns {@code true} if the system is a Mac OS.
     *
     * @deprecated call {@link OS#isMac()} directly
     * @return {@code true} if the current system is Mac.
     */
    @Deprecated(since = "5.4")
    public static boolean isMac() {
        return OS.isMac();
    }

    /**
     * Returns {@code true} if the system is a Windows.
     *
     * @deprecated call {@link OS#isWindows()} directly
     * @return {@code true} if the current system is a Windows one.
     */
    @Deprecated(since = "5.4")
    public static boolean isWindows() {
        return OS.isWindows();
    }
}
