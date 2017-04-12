/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

public final class TestEnvironment {

    public static final String HAZELCAST_TEST_USE_NETWORK = "hazelcast.test.use.network";
    public static final String EXECUTE_COMPATIBILITY_TESTS = "hazelcast.test.compatibility";

    private TestEnvironment() {
    }

    public static boolean isMockNetwork() {
        return !Boolean.getBoolean(HAZELCAST_TEST_USE_NETWORK);
    }

    /**
     * @return {@code true} when compatibility tests are to be executed on a mixed version cluster
     */
    public static boolean isRunningCompatibilityTest() {
        return Boolean.getBoolean(EXECUTE_COMPATIBILITY_TESTS);
    }
}
