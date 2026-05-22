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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.sleepAtLeastMillis;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static org.assertj.core.api.Assertions.assertThat;

abstract class BaseClassUnderTest {
    static final Map<String, String> testToThreadName = new ConcurrentHashMap<>();

    private static final String SUFFIX = "-property";
    private static final String PARENT_VALUE = "PARENT_VALUE";
    private final String name = getClass().getSimpleName() + SUFFIX;

    @BeforeAll
    static void beforeAll(TestInfo testInfo) {
        System.setProperty(testInfo.getTestClass().orElseThrow().getSimpleName() + SUFFIX, PARENT_VALUE);
    }

    @AfterAll
    static void afterAll(TestInfo testInfo) {
        assertThat(System.getProperty(testInfo.getTestClass().orElseThrow().getSimpleName() + SUFFIX)).isEqualTo(PARENT_VALUE);
        System.clearProperty(testInfo.getTestClass().orElseThrow().getSimpleName() + SUFFIX);
    }

    @Test
    void test1() {
        testToThreadName.put("test1", Thread.currentThread().getName());
        testSystemProperties(name);
        sleepSeconds(1);
    }

    @Test
    void test2() {
        testToThreadName.put("test2", Thread.currentThread().getName());
        testSystemProperties(name);
        sleepSeconds(1);
    }

    @RepeatedTest(2)
    void test3(RepetitionInfo repInfo) {
        testToThreadName.put("test3-" + repInfo.getCurrentRepetition(), Thread.currentThread().getName());
        testSystemProperties(name);
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MILLISECONDS)
    void timedOut() {
        testToThreadName.put("timedOut", Thread.currentThread().getName());
        sleepSeconds(2);
    }

    /**
     * Checks that if a test writes a value to System Properties, this value won't get overridden by other tests.
     * <p>
     * For serial execution testing we need only one invocation, but for parallel tests we do few iterations
     * to make it possible for tests to override each other's values.
     */
    private void testSystemProperties(String name) {
        assertThat(System.getProperty(name)).isEqualTo(PARENT_VALUE);
        int count = parallel() ? 137 : 1;
        for (int i = 0; i < count; i++) {
            System.setProperty(name, "test1");
            sleepAtLeastMillis(10);
            assertThat(System.getProperty(name)).isEqualTo("test1");
        }
    }

    abstract boolean parallel();
}
