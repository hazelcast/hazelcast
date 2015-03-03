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

package com.hazelcast.spi.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class SlowOperationDetectorBasicTest extends SlowOperationDetectorAbstractTest {

    @Test
    public void testSlowRunnableOnGenericOperationThread() {
        HazelcastInstance instance = getSingleNodeCluster(1000);

        getInternalOperationService(instance).execute(new SlowRunnable(3, Operation.GENERIC_PARTITION_ID));
        waitForAllOperationsToComplete(instance);

        Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
        assertNumberOfSlowOperationLogs(logs, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, 1);
        assertOperationContainsClassName(firstLog, "SlowRunnable");
        assertStackTraceContainsClassName(firstLog, "SlowRunnable");
    }

    @Test
    public void testSlowRunnableOnPartitionOperationThread() {
        HazelcastInstance instance = getSingleNodeCluster(1000);

        getInternalOperationService(instance).execute(new SlowRunnable(3, 1));
        waitForAllOperationsToComplete(instance);

        Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
        assertNumberOfSlowOperationLogs(logs, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, 1);
        assertOperationContainsClassName(firstLog, "SlowRunnable");
        assertStackTraceContainsClassName(firstLog, "SlowRunnable");
    }

    @Test
    public void testSlowOperationOnGenericOperationThread() {
        HazelcastInstance instance = getSingleNodeCluster(1000);

        executeOperation(instance, new SlowOperation(2));
        waitForAllOperationsToComplete(instance);

        Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
        assertNumberOfSlowOperationLogs(logs, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, 1);
        assertOperationContainsClassName(firstLog, "SlowOperation");
        assertStackTraceContainsClassName(firstLog, "SlowOperation");
    }

    @Test
    public void testSlowOperationOnPartitionOperationThread() {
        HazelcastInstance instance = getSingleNodeCluster(1000);

        executeOperation(instance, new SlowOperation(4, 1));
        waitForAllOperationsToComplete(instance);

        Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
        assertNumberOfSlowOperationLogs(logs, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, 1);
        assertOperationContainsClassName(firstLog, "SlowOperation");
        assertStackTraceContainsClassName(firstLog, "SlowOperation");
    }

    @Test
    public void testNestedSlowOperationOnSamePartition() {
        HazelcastInstance instance = getSingleNodeCluster(1000);
        IMap<String, String> map = getMapWithSingleElement(instance);

        int partitionId = getDefaultPartitionId(instance);
        executeOperation(instance, new NestedSlowOperationOnSamePartition(map, partitionId, 3));
        waitForAllOperationsToComplete(instance);

        Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
        assertNumberOfSlowOperationLogs(logs, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, 1);
        assertOperationContainsClassName(firstLog, "NestedSlowOperationOnSamePartition");
        assertStackTraceContainsClassName(firstLog, "NestedSlowOperationOnSamePartition");
        assertStackTraceContainsClassName(firstLog, "SlowEntryProcessor");
    }

    @Test
    public void testNestedSlowOperationOnPartitionAndGenericOperationThreads() {
        HazelcastInstance instance = getSingleNodeCluster(1000);

        executeOperation(instance, new NestedSlowOperationOnPartitionAndGenericOperationThreads(instance, 3));
        waitForAllOperationsToComplete(instance);

        Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
        assertNumberOfSlowOperationLogs(logs, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, 1);
        assertStackTraceNotContainsClassName(firstLog, "NestedSlowOperation");
        assertStackTraceContainsClassName(firstLog, "SlowOperation");
    }

    @Test
    public void testSlowRecursiveOperation() {
        int partitionThreads = 32;
        int numberOfOperations = 50;
        int recursionDepth = 20;

        Config config = new Config();
        config.setProperty(GroupProperties.PROP_SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS, "1000");
        config.setProperty(GroupProperties.PROP_PARTITION_OPERATION_THREAD_COUNT, String.valueOf(partitionThreads));

        final HazelcastInstance instance = getSingleNodeCluster(config);

        int partitionCount = TestUtil.getNode(instance).nodeEngine.getPartitionService().getPartitionCount();
        int partitionIndex = 1;
        for (int i = 0; i < numberOfOperations; i++) {
            int partitionId = partitionIndex % partitionCount;
            executeOperation(instance, new SlowRecursiveOperation(partitionId, recursionDepth, 10));
            partitionIndex += partitionCount / partitionThreads + 1;
        }
        waitForAllOperationsToComplete(instance);

        Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
        assertNumberOfSlowOperationLogs(logs, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, numberOfOperations);
        assertStackTraceContainsClassName(firstLog, "SlowRecursiveOperation");
    }

    private static class SlowRunnable implements PartitionSpecificRunnable {

        private final int sleepSeconds;
        private final int partitionId;

        private SlowRunnable(int sleepSeconds, int partitionId) {
            this.sleepSeconds = sleepSeconds;
            this.partitionId = partitionId;
        }

        @Override
        public void run() {
            sleepSeconds(sleepSeconds);
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }
    }

    private static class SlowOperation extends AbstractOperation {

        private final int sleepSeconds;

        private SlowOperation(int sleepSeconds) {
            this.sleepSeconds = sleepSeconds;
        }

        private SlowOperation(int sleepSeconds, int partitionId) {
            this.sleepSeconds = sleepSeconds;
            this.setPartitionId(partitionId);
        }

        @Override
        public void run() throws Exception {
            sleepSeconds(sleepSeconds);
        }
    }

    private static class NestedSlowOperationOnSamePartition extends AbstractOperation {

        private final IMap<String, String> map;
        private final int sleepSeconds;

        private NestedSlowOperationOnSamePartition(IMap<String, String> map, int partitionId, int sleepSeconds) {
            this.map = map;
            this.sleepSeconds = sleepSeconds;

            this.setPartitionId(partitionId);
        }

        @Override
        public void run() throws Exception {
            executeEntryProcessor(map, new SlowEntryProcessor(sleepSeconds));
        }
    }

    private static class NestedSlowOperationOnPartitionAndGenericOperationThreads extends AbstractOperation {

        private final HazelcastInstance instance;
        private final int sleepSeconds;

        private NestedSlowOperationOnPartitionAndGenericOperationThreads(HazelcastInstance instance, int sleepSeconds) {
            this.instance = instance;
            this.sleepSeconds = sleepSeconds;

            int partitionId = getDefaultPartitionId(instance);
            setPartitionId(partitionId);
        }

        @Override
        public void run() throws Exception {
            getInternalOperationService(instance)
                    .executeOperation(new SlowOperation(sleepSeconds, GENERIC_PARTITION_ID));
        }
    }

    private static class SlowRecursiveOperation extends AbstractOperation {
        private final int recursionDepth;
        private final int sleepSeconds;

        public SlowRecursiveOperation(int partitionId, int recursionDepth, int sleepSeconds) {
            this.recursionDepth = recursionDepth;
            this.sleepSeconds = sleepSeconds;

            setPartitionId(partitionId);
        }

        @Override
        public void run() throws Exception {
            recursiveCall(recursionDepth);
        }

        void recursiveCall(int depth) {
            if (depth == 0) {
                sleepSeconds(sleepSeconds);
                return;
            }
            recursiveCall(depth - 1);
        }
    }
}
