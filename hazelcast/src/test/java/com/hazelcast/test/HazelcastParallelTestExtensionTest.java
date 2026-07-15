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

class HazelcastParallelTestExtensionTest {

    @Test
    @SetSystemProperty(key = "hazelcast.test.enableExtensionTest", value = "true")
    void executesTestInParallel() {
        assumeThat(ForkJoinPool.getCommonPoolParallelism()).isGreaterThanOrEqualTo(2);
        BaseClassUnderTest.setProperties(ClassWithParallelTest.class);
        EngineTestKit.engine("junit-jupiter")//
                     .selectors(selectClass(ClassWithParallelTest.class))
                     .configurationParameter("junit.jupiter.execution.parallel.enabled", "true")
                     .execute()
                     .testEvents()
                     .debug()
                     .assertStatistics(stats -> stats.started(5).succeeded(4).failed(1));

        System.out.println("Test to thread map: " + ClassWithParallelTest.testToThreadName);

        BaseClassUnderTest.assertPropertiesRestored(ClassWithParallelTest.class);
        assertThat(ClassWithParallelTest.testToThreadName).hasSize(5);
        // it was not all in the same thread
        assertThat(new HashSet<>(ClassWithParallelTest.testToThreadName.values())).hasSizeGreaterThan(2);
        assertThat(System.getProperties().keySet()).noneMatch(key -> ((String) key).contains("ClassUnderTests"));
    }

    @ParallelTest
    @EnabledIfSystemProperty(named = "hazelcast.test.enableExtensionTest", matches = "true")
    static class ClassWithParallelTest extends BaseClassUnderTest {

        @Override
        boolean parallel() {
            return true;
        }
    }

}
