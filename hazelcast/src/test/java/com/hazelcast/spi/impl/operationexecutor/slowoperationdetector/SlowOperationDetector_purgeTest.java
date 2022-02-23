/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationexecutor.slowoperationdetector;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class SlowOperationDetector_purgeTest extends SlowOperationDetectorAbstractTest {

    private HazelcastInstance instance;
    private IMap<String, String> map;

    private void setup(String logRetentionSeconds) {
        Config config = new Config();
        config.setProperty(ClusterProperty.SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS.getName(), "800");
        config.setProperty(ClusterProperty.SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS.getName(), logRetentionSeconds);
        config.setProperty(ClusterProperty.SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS.getName(), "1");
        config.setProperty(ClusterProperty.SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED.getName(), "true");

        instance = createHazelcastInstance(config);
        map = getMapWithSingleElement(instance);
    }

    @After
    public void teardown() {
        shutdownOperationService(instance);
        shutdownNodeFactory();
    }

    @Test
    public void testPurging_Invocation() {
        setup("3");

        // all of these entry processors are executed after each other, not in parallel
        for (int i = 0; i < 3; i++) {
            map.executeOnEntries(getSlowEntryProcessor(3));
        }
        map.executeOnEntries(getSlowEntryProcessor(5));
        awaitSlowEntryProcessors();

        // shutdown to stop purging, so the last one or two entry processor invocations will survive
        shutdownOperationService(instance);

        Collection<SlowOperationLog> logs = getSlowOperationLogsAndAssertNumberOfSlowOperationLogs(instance, 1);
        SlowOperationLog firstLog = logs.iterator().next();

        assertEntryProcessorOperation(firstLog);
        assertStackTraceContainsClassName(firstLog, "SlowEntryProcessor");

        Collection<SlowOperationLog.Invocation> invocations = getInvocations(firstLog);
        int invocationCount = invocations.size();
        int totalInvocations = firstLog.totalInvocations.get();
        assertTrue(String.format("Expected invocations must be less than total invocations, but was %s, total invocations: %s",
                invocationCount, totalInvocations), invocationCount < totalInvocations);
        for (SlowOperationLog.Invocation invocation : invocations) {
            assertInvocationDurationBetween(invocation, 1000, 4500);
        }
    }

    @Test
    public void testPurging_SlowOperationLog() {
        setup("2");

        // all of these entry processors are executed after each other, not in parallel
        for (int i = 0; i < 2; i++) {
            map.executeOnEntries(getSlowEntryProcessor(3));
        }
        awaitSlowEntryProcessors();

        // sleep a bit to purge the last entry processor invocation (and the whole slow operation log with it)
        sleepSeconds(3);
        shutdownOperationService(instance);

        getSlowOperationLogsAndAssertNumberOfSlowOperationLogs(instance, 0);
    }
}
