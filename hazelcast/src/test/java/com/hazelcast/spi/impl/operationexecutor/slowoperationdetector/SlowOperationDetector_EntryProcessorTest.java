/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static java.lang.String.format;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class SlowOperationDetector_EntryProcessorTest extends SlowOperationDetectorAbstractTest {

    @Test
    public void testSlowEntryProcessor() throws InterruptedException {
        HazelcastInstance instance = getSingleNodeCluster(2000);
        IMap<String, String> map = getMapWithSingleElement(instance);

        for (int i = 0; i < 3; i++) {
            map.executeOnEntries(new SlowEntryProcessor(3));
        }
        map.executeOnEntries(new SlowEntryProcessor(6));
        waitForAllOperationsToComplete(instance);

        Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
        assertNumberOfSlowOperationLogs(logs, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, 4);
        assertEntryProcessorOperation(firstLog);
        assertStackTraceContainsClassName(firstLog, "SlowEntryProcessor");

        Collection<SlowOperationLog.Invocation> invocations = firstLog.getInvocations();
        assertEqualsStringFormat("Expected %d invocations, but was %d", 4, invocations.size());
        for (SlowOperationLog.Invocation invocation : invocations) {
            assertDurationBetween(invocation.getDurationNanos(), 2000, 6500);
        }
    }

    @Test
    public void testMultipleSlowEntryProcessorClasses() throws InterruptedException {
        HazelcastInstance instance = getSingleNodeCluster(2000);
        IMap<String, String> map = getMapWithSingleElement(instance);

        for (int i = 0; i < 3; i++) {
            map.executeOnEntries(new SlowEntryProcessor(3));
        }
        map.executeOnEntries(new SlowEntryProcessorChild(3));
        map.executeOnEntries(new SlowEntryProcessor(5));
        waitForAllOperationsToComplete(instance);

        Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
        assertNumberOfSlowOperationLogs(logs, 2);

        Iterator<SlowOperationLog> iterator = logs.iterator();
        int firstSize = iterator.next().getInvocations().size();
        int secondSize = iterator.next().getInvocations().size();
        assertTrue(format("Expected to find 1 and 4 invocations in logs, but was %d and %d", firstSize, secondSize),
                (firstSize == 1 ^ secondSize == 1) && (firstSize == 4 ^ secondSize == 4));
    }

    @Test
    public void testNestedSlowEntryProcessor() {
        HazelcastInstance instance = getSingleNodeCluster(1000);
        IMap<String, String> map = getMapWithSingleElement(instance);

        map.executeOnEntries(new NestedSlowEntryProcessor(map, 2));
        waitForAllOperationsToComplete(instance);

        Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
        assertNumberOfSlowOperationLogs(logs, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, 1);
        assertEntryProcessorOperation(firstLog);
        assertStackTraceContainsClassName(firstLog, "NestedSlowEntryProcessor");
        assertStackTraceContainsClassName(firstLog, "SlowEntryProcessor");
    }

    private static class NestedSlowEntryProcessor implements EntryProcessor<String, String> {

        final IMap<String, String> map;
        final int sleepSeconds;

        private NestedSlowEntryProcessor(IMap<String, String> map, int sleepSeconds) {
            this.map = map;
            this.sleepSeconds = sleepSeconds;
        }

        @Override
        public Object process(Map.Entry<String, String> entry) {
            executeEntryProcessor(map, new SlowEntryProcessor(sleepSeconds));
            return null;
        }

        @Override
        public EntryBackupProcessor<String, String> getBackupProcessor() {
            return null;
        }
    }
}
