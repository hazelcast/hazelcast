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

package com.hazelcast.test.bounce;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.ParallelTest;
import com.hazelcast.test.SerialTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.util.SetSystemProperty;
import org.junit.platform.testkit.engine.EngineTestKit;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.bounce.BounceTestConfiguration.DriverType.ALWAYS_UP_MEMBER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.testkit.engine.EventConditions.event;
import static org.junit.platform.testkit.engine.EventConditions.finishedSuccessfully;
import static org.junit.platform.testkit.engine.EventConditions.finishedWithFailure;
import static org.junit.platform.testkit.engine.EventConditions.started;
import static org.junit.platform.testkit.engine.EventConditions.test;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.instanceOf;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.message;

@SerialTest
@QuickTest
class BounceMemberExtensionTest {

    @Test
    @SetSystemProperty(key = "hazelcast.test.enableBMRTest", value = "true")
    void lifecycleAndFailures() {
        LifecycleTest.tearDownCount = 0;
        var results = EngineTestKit.engine("junit-jupiter")
                .selectors(selectClass(LifecycleTest.class))
                .execute();
        results.testEvents().assertThatEvents()
               .haveExactly(1, event(test("tasksAndBouncing"), finishedSuccessfully()))
               .haveExactly(1, event(test("taskFailure"),
                                     finishedWithFailure(instanceOf(IllegalStateException.class), message("task failure"))))
               .haveExactly(1, event(test("setupFailure"),
                                     finishedWithFailure(message("setup failure"))));
        assertEquals(3, LifecycleTest.tearDownCount);
    }

    @Test
    @SetSystemProperty(key = "hazelcast.test.enableBMRTest", value = "true")
    void cleansUpAfterDriverCreationFails() {
        EngineTestKit.engine("junit-jupiter")
                     .selectors(selectClass(FailingSetupTest.class))
                     .execute()
                     .testEvents()
                     .assertThatEvents()
                     .haveExactly(1, event(test("test"), started()))
                     .haveExactly(1, event(test("test"),
                                           finishedWithFailure(message("driver creation failure"))));
        for (HazelcastInstance member : FailingSetupTest.members) {
            assertFalse(member.getLifecycleService().isRunning());
        }
    }

    @Test
    @SetSystemProperty(key = "hazelcast.test.enableBMRTest", value = "true")
    void doesNotExecuteInParallel() {
        EngineTestKit.engine("junit-jupiter")
                     .selectors(selectClass(ParallelExecutionTest.class))
                     .execute()
                     .testEvents()
                     .assertThatEvents()
                     .haveExactly(1, event(test("test"), started()))
                     .haveExactly(1, event(test("test"), finishedWithFailure(message(
                         "BounceMemberExtension cannot be used in ParallelTest. "
                             + "Please use @SerialTest annotation instead."))));
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    @EnabledIfSystemProperty(named = "hazelcast.test.enableBMRTest", matches = "true")
    static class LifecycleTest {
        static int tearDownCount;
        private HazelcastInstance[] previousMembers;
        private HazelcastInstance bouncingMember;

        @RegisterExtension
        final BounceMemberExtension bounce = BounceMemberExtension.with(Config::new)
                .clusterSize(2).driverType(ALWAYS_UP_MEMBER).driverCount(1)
                .useTerminate(true).bouncingIntervalSeconds(1).build();

        @BeforeEach
        void setup(TestInfo info) {
            if (previousMembers != null) {
                for (HazelcastInstance member : previousMembers) {
                    assertFalse(member.getLifecycleService().isRunning());
                }
            }
            previousMembers = bounce.getMembersSnapshot();
            assertTrue(bounce.getSteadyMember().getLifecycleService().isRunning());
            bouncingMember = bounce.getMembers().get(1);
            assertTrue(bouncingMember.getLifecycleService().isRunning());
            assertSame(bounce.getSteadyMember(), bounce.getNextTestDriver());
            if (info.getTestMethod().orElseThrow().getName().equals("setupFailure")) {
                throw new IllegalStateException("setup failure");
            }
        }

        @Test
        void tasksAndBouncing() {
            assertTrueEventually(() -> assertFalse(bouncingMember.getLifecycleService().isRunning()));
            bounce.test(new Runnable[]{() -> bounce.getNextTestDriver().getMap("test").put("key", "value")});
            assertEquals("value", bounce.getSteadyMember().getMap("test").get("key"));
            assertThrows(AssertionError.class, () -> bounce.test(new Runnable[]{() -> { }}));
        }

        @Test
        void taskFailure() {
            bounce.testRepeatedly(1, () -> {
                throw new IllegalStateException("task failure");
            }, 10);
        }

        @Test
        void setupFailure() {
            throw new AssertionError("Test must not run after setup failure");
        }

        @AfterEach
        void tearDown() {
            assertTrue(bounce.getSteadyMember().getLifecycleService().isRunning());
            tearDownCount++;
        }

        @AfterAll
        void verifyShutdown() {
            for (HazelcastInstance member : bounce.getMembersSnapshot()) {
                assertFalse(member.getLifecycleService().isRunning());
            }
        }
    }

    @EnabledIfSystemProperty(named = "hazelcast.test.enableBMRTest", matches = "true")
    static class FailingSetupTest {
        static HazelcastInstance[] members;

        @RegisterExtension
        final BounceMemberExtension bounce = BounceMemberExtension.with(new Config())
                .clusterSize(2).driverType(ALWAYS_UP_MEMBER).driverCount(1)
                .driverFactory(rule -> {
                    members = rule.getMembersSnapshot();
                    throw new IllegalStateException("driver creation failure");
                }).build();

        @Test
        void test() {
            throw new AssertionError("Test must not run after driver creation failure");
        }
    }

    @EnabledIfSystemProperty(named = "hazelcast.test.enableBMRTest", matches = "true")
    @ParallelTest
    static class ParallelExecutionTest {

        @RegisterExtension
        final BounceMemberExtension bounce = BounceMemberExtension.with(new Config())
                                                                  .clusterSize(2).driverType(ALWAYS_UP_MEMBER).driverCount(1)
                                                                  .build();

        @Test
        void test() {
        }
    }
}
