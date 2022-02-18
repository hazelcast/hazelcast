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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
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
    public void testSlowEntryProcessor() {
        HazelcastInstance instance = getSingleNodeCluster(1000);
        IMap<String, String> map = getMapWithSingleElement(instance);

        for (int i = 0; i < 3; i++) {
            map.executeOnEntries(getSlowEntryProcessor(3));
        }
        map.executeOnEntries(getSlowEntryProcessor(6));
        awaitSlowEntryProcessors();

        Collection<SlowOperationLog> logs = getSlowOperationLogsAndAssertNumberOfSlowOperationLogs(instance, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, 4);
        assertEntryProcessorOperation(firstLog);
        assertStackTraceContainsClassName(firstLog, "SlowEntryProcessor");

        Collection<SlowOperationLog.Invocation> invocations = getInvocations(firstLog);
        assertEqualsStringFormat("Expected %d invocations, but was %d", 4, invocations.size());
        for (SlowOperationLog.Invocation invocation : invocations) {
            assertInvocationDurationBetween(invocation, 1000, 6500);
        }
    }

    @Test
    public void testMultipleSlowEntryProcessorClasses() {
        HazelcastInstance instance = getSingleNodeCluster(1000);
        IMap<String, String> map = getMapWithSingleElement(instance);

        for (int i = 0; i < 3; i++) {
            map.executeOnEntries(getSlowEntryProcessor(3));
        }
        SlowEntryProcessorChild entryProcessorChild = new SlowEntryProcessorChild(3);
        map.executeOnEntries(entryProcessorChild);
        map.executeOnEntries(getSlowEntryProcessor(5));

        awaitSlowEntryProcessors();
        entryProcessorChild.await();

        Collection<SlowOperationLog> logs = getSlowOperationLogsAndAssertNumberOfSlowOperationLogs(instance, 2);

        Iterator<SlowOperationLog> iterator = logs.iterator();
        SlowOperationLog firstLog = iterator.next();
        SlowOperationLog secondLog = iterator.next();
        Collection<SlowOperationLog.Invocation> firstInvocations = getInvocations(firstLog);
        Collection<SlowOperationLog.Invocation> secondInvocations = getInvocations(secondLog);

        int firstSize = firstInvocations.size();
        int secondSize = secondInvocations.size();
        assertTrue(format(
                "Expected to find 1 and 4 invocations in logs, but was %d and %d. First log: %s%nSecond log: %s",
                firstSize, secondSize, firstLog.createDTO().toJson(), secondLog.createDTO().toJson()),
                (firstSize == 1 ^ secondSize == 1) && (firstSize == 4 ^ secondSize == 4));

        for (SlowOperationLog.Invocation invocation : firstInvocations) {
            assertInvocationDurationBetween(invocation, 1000, 5500);
        }
        for (SlowOperationLog.Invocation invocation : secondInvocations) {
            assertInvocationDurationBetween(invocation, 1000, 5500);
        }
    }

    @Test
    public void testNestedSlowEntryProcessor() {
        HazelcastInstance instance = getSingleNodeCluster(1000);
        IMap<String, String> map = getMapWithSingleElement(instance);

        NestedSlowEntryProcessor entryProcessor = new NestedSlowEntryProcessor(map, 3);
        map.executeOnEntries(entryProcessor);
        entryProcessor.await();

        Collection<SlowOperationLog> logs = getSlowOperationLogsAndAssertNumberOfSlowOperationLogs(instance, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, 1);
        assertEntryProcessorOperation(firstLog);
        assertStackTraceContainsClassName(firstLog, "NestedSlowEntryProcessor");
        assertStackTraceContainsClassName(firstLog, "SlowEntryProcessor");
    }

    private static class NestedSlowEntryProcessor implements EntryProcessor<String, String, Object> {

        private final IMap<String, String> map;
        private final SlowEntryProcessor entryProcessor;

        private NestedSlowEntryProcessor(IMap<String, String> map, int sleepSeconds) {
            this.map = map;
            this.entryProcessor = new SlowEntryProcessor(sleepSeconds);
        }

        @Override
        public Object process(Map.Entry<String, String> entry) {
            executeEntryProcessor(map, entryProcessor);
            return null;
        }

        @Override
        public EntryProcessor<String, String, Object> getBackupProcessor() {
            return null;
        }

        private void await() {
            entryProcessor.await();
        }
    }
}
