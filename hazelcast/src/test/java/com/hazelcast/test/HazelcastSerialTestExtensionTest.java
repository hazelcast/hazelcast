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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.util.SetSystemProperty;
import org.junit.platform.testkit.engine.EngineTestKit;

import java.util.HashSet;
import java.util.concurrent.ForkJoinPool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

class HazelcastSerialTestExtensionTest {

    @Test
    @SetSystemProperty(key = "hazelcast.test.enableExtensionTest", value = "true")
    void executesTestInSerial() {
        assumeThat(ForkJoinPool.getCommonPoolParallelism()).isGreaterThanOrEqualTo(2);
        BaseClassUnderTest.setProperties(ClassWithSerialTest.class);
        EngineTestKit.engine("junit-jupiter")//
                     .selectors(selectClass(ClassWithSerialTest.class))
                     .configurationParameter("junit.jupiter.execution.parallel.enabled", "true")
                     .execute()
                     .testEvents()
                     .debug()
                     .assertStatistics(stats -> stats.started(5).succeeded(4).failed(1));

        System.out.println("Test to thread map: " + ClassWithSerialTest.testToThreadName);
        BaseClassUnderTest.assertPropertiesRestored(ClassWithSerialTest.class);

        assertThat(ClassWithSerialTest.testToThreadName).hasSize(5);
        // it was all in the same thread
        assertThat(new HashSet<>(ClassWithSerialTest.testToThreadName.values())).hasSize(1);

        assertThat(System.getProperties().keySet()).noneMatch(key -> ((String) key).contains("ClassUnderTests"));
    }

    @SerialTest
    @EnabledIfSystemProperty(named = "hazelcast.test.enableExtensionTest", matches = "true")
    static class ClassWithSerialTest extends BaseClassUnderTest {

        @Override
        boolean parallel() {
            return false;
        }
    }
}
