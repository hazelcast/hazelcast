/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

/**
 * Keeps current test name in a {@link InheritableThreadLocal}.
 */
public final class TestNameHolder {
    private static final ThreadLocal<String> TEST_NAME_THREAD_LOCAL = new InheritableThreadLocal<>();

    private TestNameHolder() {
    }

    /**
     * Gets the name of current test.
     * @see AbstractHazelcastClassRunner
     * @see AbstractHazelcastExtension
     */
    public static String getTestMethodName() {
        return TEST_NAME_THREAD_LOCAL.get();
    }

    static void setThreadLocalTestMethodName(String name) {
        TestLoggingUtils.setThreadLocalTestMethodName(name);
        TEST_NAME_THREAD_LOCAL.set(name);
    }

    static void removeThreadLocalTestMethodName() {
        TestLoggingUtils.removeThreadLocalTestMethodName();
        TEST_NAME_THREAD_LOCAL.remove();
    }
}
