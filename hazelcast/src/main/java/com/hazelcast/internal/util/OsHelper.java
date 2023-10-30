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
 */
public final class OsHelper extends OS {
    /**
     * OS name in lower case.
     */
    private static final String OS_NAME_LOWER_CASE = osName().toLowerCase();

    private OsHelper() {
        super();
    }

    /**
     * Returns {@code true} if the system is from Unix family.
     *
     * @return {@code true} if the current system is Unix/Linux/AIX.
     */
    public static boolean isUnixFamily() {
        return (OS_NAME_LOWER_CASE.contains("nix") || OS_NAME_LOWER_CASE.contains("nux") || OS_NAME_LOWER_CASE.contains("aix"));
    }
}
