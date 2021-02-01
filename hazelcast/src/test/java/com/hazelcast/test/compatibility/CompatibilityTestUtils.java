/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.compatibility;

public final class CompatibilityTestUtils {

    /**
     * System property to override the other hazelcast version to be used by
     * compatibility tests running with other releases.
     * <p>
     * Set this system property to a single version,
     * e.g. {@code -Dhazelcast.test.compatibility.otherVersion=4.0}.
     */
    public static final String COMPATIBILITY_TEST_OTHER_VERSION = "hazelcast.test.compatibility.otherVersion";

    private static final String DEFAULT_OTHER_VERSION = "3.12.8-migration";

    /**
     * Resolves which version will be used for compatibility tests using the next
     * Hazelcast release.
     * <ol>
     * <li>look for system property override</li>
     * <li>fallback to 4.0</li>
     * </ol>
     */
    public static String resolveOtherVersion() {
        String systemPropertyOverride = System.getProperty(COMPATIBILITY_TEST_OTHER_VERSION);
        return systemPropertyOverride != null ? systemPropertyOverride : DEFAULT_OTHER_VERSION;
    }
}
