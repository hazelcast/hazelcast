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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SlowOperationDetectorBasicTest extends SlowOperationDetectorAbstractTest {

    private HazelcastInstance instance;

    @After
    public void teardown() {
        shutdownOperationService(instance);
        shutdownNodeFactory();
    }

    @Test
    public void testDisabled() {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_SLOW_OPERATION_DETECTOR_ENABLED, "false");
        config.setProperty(GroupProperties.PROP_SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS, "1000");

        instance = createHazelcastInstance(config);

        SlowRunnable runnable = new SlowRunnable(3, Operation.GENERIC_PARTITION_ID);
        getOperationService(instance).execute(runnable);
        runnable.await();

        Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
        assertNumberOfSlowOperationLogs(logs, 0);
    }

    @Test
    public void testSlowRunnableOnGenericOperationThread() {
        instance = getSingleNodeCluster(1000);

        SlowRunnable runnable = new SlowRunnable(3, Operation.GENERIC_PARTITION_ID);
        getOperationService(instance).execute(runnable);
        runnable.await();

        Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
        assertNumberOfSlowOperationLogs(logs, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, 1);
        assertOperationContainsClassName(firstLog, "SlowRunnable");
        assertStackTraceContainsClassName(firstLog, "SlowRunnable");
    }

    @Test
    public void testSlowRunnableOnPartitionOperationThread() {
        instance = getSingleNodeCluster(1000);

        SlowRunnable runnable = new SlowRunnable(3, 1);
        getOperationService(instance).execute(runnable);
        runnable.await();

        Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
        assertNumberOfSlowOperationLogs(logs, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, 1);
        assertOperationContainsClassName(firstLog, "SlowRunnable");
        assertStackTraceContainsClassName(firstLog, "SlowRunnable");
    }

    @Test
    public void testSlowOperationOnGenericOperationThread() {
        instance = getSingleNodeCluster(1000);

        SlowOperation operation = new SlowOperation(3);
        executeOperation(instance, operation);
        operation.await();

        Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
        assertNumberOfSlowOperationLogs(logs, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, 1);
        assertOperationContainsClassName(firstLog, "SlowOperation");
        assertStackTraceContainsClassName(firstLog, "SlowOperation");
    }

    @Test
    public void testSlowOperationOnPartitionOperationThread() {
        instance = getSingleNodeCluster(1000);

        SlowOperation operation = new SlowOperation(4, 2);
        executeOperation(instance, operation);
        operation.await();

        Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
        assertNumberOfSlowOperationLogs(logs, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, 1);
        assertOperationContainsClassName(firstLog, "SlowOperation");
        assertStackTraceContainsClassName(firstLog, "SlowOperation");
    }

    @Test
    public void testNestedSlowOperationOnSamePartition() {
        instance = getSingleNodeCluster(1000);
        IMap<String, String> map = getMapWithSingleElement(instance);

        int partitionId = getDefaultPartitionId(instance);
        NestedSlowOperationOnSamePartition operation = new NestedSlowOperationOnSamePartition(map, partitionId, 3);
        executeOperation(instance, operation);
        operation.await();

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
        instance = getSingleNodeCluster(1000);

        NestedSlowOperationOnPartitionAndGenericOperationThreads operation
                = new NestedSlowOperationOnPartitionAndGenericOperationThreads(instance, 3);
        executeOperation(instance, operation);
        operation.await();

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
        int numberOfOperations = 40;
        int recursionDepth = 15;

        Config config = new Config();
        config.setProperty(GroupProperties.PROP_SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS, "1000");
        config.setProperty(GroupProperties.PROP_SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS, String.valueOf(Integer.MAX_VALUE));
        config.setProperty(GroupProperties.PROP_PARTITION_OPERATION_THREAD_COUNT, String.valueOf(partitionThreads));
        instance = createHazelcastInstance(config);

        List<SlowRecursiveOperation> operations = new ArrayList<SlowRecursiveOperation>(numberOfOperations);
        int partitionCount = getNode(instance).nodeEngine.getPartitionService().getPartitionCount();

        int partitionIndex = 1;
        for (int i = 0; i < numberOfOperations; i++) {
            int partitionId = partitionIndex % partitionCount;
            SlowRecursiveOperation operation = new SlowRecursiveOperation(partitionId, recursionDepth, 4);
            operations.add(operation);
            executeOperation(instance, operation);
            partitionIndex += partitionCount / partitionThreads + 1;
        }

        for (SlowRecursiveOperation operation : operations) {
            operation.await();
        }

        Collection<SlowOperationLog> logs = getSlowOperationLogs(instance);
        assertNumberOfSlowOperationLogs(logs, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, numberOfOperations);
        assertStackTraceContainsClassName(firstLog, "SlowRecursiveOperation");
    }

    private static class SlowRunnable extends CountDownLatchHolder implements PartitionSpecificRunnable {

        private final int sleepSeconds;
        private final int partitionId;

        private SlowRunnable(int sleepSeconds, int partitionId) {
            this.sleepSeconds = sleepSeconds;
            this.partitionId = partitionId;
        }

        @Override
        public void run() {
            sleepSeconds(sleepSeconds);
            done();
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }
    }

    private static class SlowOperation extends CountDownLatchOperation {

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
            done();
        }
    }

    private static class NestedSlowOperationOnSamePartition extends AbstractOperation {

        private final IMap<String, String> map;
        private final SlowEntryProcessor entryProcessor;

        private NestedSlowOperationOnSamePartition(IMap<String, String> map, int partitionId, int sleepSeconds) {
            this.map = map;
            this.entryProcessor = new SlowEntryProcessor(sleepSeconds);

            this.setPartitionId(partitionId);
        }

        @Override
        public void run() throws Exception {
            executeEntryProcessor(map, entryProcessor);
        }

        public void await() {
            entryProcessor.await();
        }
    }

    private static class NestedSlowOperationOnPartitionAndGenericOperationThreads extends AbstractOperation {

        private final HazelcastInstance instance;
        private final SlowOperation operation;

        private NestedSlowOperationOnPartitionAndGenericOperationThreads(HazelcastInstance instance, int sleepSeconds) {
            this.instance = instance;
            this.operation = new SlowOperation(sleepSeconds, GENERIC_PARTITION_ID);

            int partitionId = getDefaultPartitionId(instance);
            setPartitionId(partitionId);
        }

        @Override
        public void run() throws Exception {
            getOperationService(instance).executeOperation(operation);
        }

        public void await() {
            operation.await();
        }
    }

    private static class SlowRecursiveOperation extends CountDownLatchOperation {

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
                done();
                return;
            }
            recursiveCall(depth - 1);
        }
    }
}
