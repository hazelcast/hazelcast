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
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.spi.impl.operationservice.Operation.GENERIC_PARTITION_ID;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_OPERATION_THREAD_COUNT;
import static com.hazelcast.spi.properties.ClusterProperty.SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS;
import static com.hazelcast.test.Accessors.getOperationService;
import static com.hazelcast.test.Accessors.getPartitionService;
import static java.lang.String.valueOf;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
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
        config.setProperty(ClusterProperty.SLOW_OPERATION_DETECTOR_ENABLED.getName(), "false");
        config.setProperty(SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS.getName(), "1000");

        instance = createHazelcastInstance(config);

        SlowRunnable runnable = new SlowRunnable(5, GENERIC_PARTITION_ID);
        getOperationService(instance).execute(runnable);
        runnable.await();

        getSlowOperationLogsAndAssertNumberOfSlowOperationLogs(instance, 0);
    }

    @Test
    public void testSlowRunnableOnGenericOperationThread() {
        instance = getSingleNodeCluster(1000);

        SlowRunnable runnable = new SlowRunnable(5, GENERIC_PARTITION_ID);
        getOperationService(instance).execute(runnable);
        runnable.await();

        Collection<SlowOperationLog> logs = getSlowOperationLogsAndAssertNumberOfSlowOperationLogs(instance, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, 1);
        assertOperationContainsClassName(firstLog, "SlowRunnable");
        assertStackTraceContainsClassName(firstLog, "SlowRunnable");
    }

    @Test
    public void testSlowRunnableOnPartitionOperationThread() {
        instance = getSingleNodeCluster(1000);

        SlowRunnable runnable = new SlowRunnable(5, 1);
        getOperationService(instance).execute(runnable);
        runnable.await();

        Collection<SlowOperationLog> logs = getSlowOperationLogsAndAssertNumberOfSlowOperationLogs(instance, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, 1);
        assertOperationContainsClassName(firstLog, "SlowRunnable");
        assertStackTraceContainsClassName(firstLog, "SlowRunnable");
    }

    @Test
    public void testSlowOperationOnGenericOperationThread() {
        instance = getSingleNodeCluster(1000);

        SlowOperation operation = new SlowOperation(5);
        executeOperation(instance, operation);
        operation.join();

        Collection<SlowOperationLog> logs = getSlowOperationLogsAndAssertNumberOfSlowOperationLogs(instance, 1);

        SlowOperationLog firstLog = logs.iterator().next();
        assertTotalInvocations(firstLog, 1);
        assertOperationContainsClassName(firstLog, "SlowOperation");
        assertStackTraceContainsClassName(firstLog, "SlowOperation");
    }

    @Test
    public void testSlowOperationOnPartitionOperationThread() {
        instance = getSingleNodeCluster(1000);

        SlowOperation operation = new SlowOperation(5, 2);
        executeOperation(instance, operation);
        operation.join();

        Collection<SlowOperationLog> logs = getSlowOperationLogsAndAssertNumberOfSlowOperationLogs(instance, 1);

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
        NestedSlowOperationOnSamePartition operation = new NestedSlowOperationOnSamePartition(map, partitionId, 5);
        executeOperation(instance, operation);
        operation.await();

        Collection<SlowOperationLog> logs = getSlowOperationLogsAndAssertNumberOfSlowOperationLogs(instance, 1);

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
                = new NestedSlowOperationOnPartitionAndGenericOperationThreads(instance, 5);
        executeOperation(instance, operation);
        operation.await();

        Collection<SlowOperationLog> logs = getSlowOperationLogsAndAssertNumberOfSlowOperationLogs(instance, 1);

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

        Config config = new Config()
                .setProperty(SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS.getName(), "1000")
                .setProperty(SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS.getName(), valueOf(Integer.MAX_VALUE))
                .setProperty(PARTITION_OPERATION_THREAD_COUNT.getName(), valueOf(partitionThreads));
        instance = createHazelcastInstance(config);

        List<SlowRecursiveOperation> operations = new ArrayList<SlowRecursiveOperation>(numberOfOperations);
        int partitionCount = getPartitionService(instance).getPartitionCount();

        int partitionIndex = 1;
        for (int i = 0; i < numberOfOperations; i++) {
            int partitionId = partitionIndex % partitionCount;
            SlowRecursiveOperation operation = new SlowRecursiveOperation(partitionId, recursionDepth, 20);
            operations.add(operation);
            executeOperation(instance, operation);
            partitionIndex += partitionCount / partitionThreads + 1;
        }

        for (SlowRecursiveOperation operation : operations) {
            operation.join();
        }

        Collection<SlowOperationLog> logs = getSlowOperationLogsAndAssertNumberOfSlowOperationLogs(instance, 1);

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

    private static class SlowOperation extends JoinableOperation {

        private final int sleepSeconds;

        private SlowOperation(int sleepSeconds) {
            this.sleepSeconds = sleepSeconds;
        }

        private SlowOperation(int sleepSeconds, int partitionId) {
            this.sleepSeconds = sleepSeconds;

            setPartitionId(partitionId);
        }

        @Override
        public void run() throws Exception {
            sleepSeconds(sleepSeconds);
            done();
        }
    }

    private static class NestedSlowOperationOnSamePartition extends Operation {

        private final IMap<String, String> map;
        private final SlowEntryProcessor entryProcessor;

        private NestedSlowOperationOnSamePartition(IMap<String, String> map, int partitionId, int sleepSeconds) {
            this.map = map;
            this.entryProcessor = new SlowEntryProcessor(sleepSeconds);

            setPartitionId(partitionId);
        }

        @Override
        public void run() throws Exception {
            executeEntryProcessor(map, entryProcessor);
        }

        public void await() {
            entryProcessor.await();
        }

        @Override
        public boolean validatesTarget() {
            return false;
        }
    }

    private static class NestedSlowOperationOnPartitionAndGenericOperationThreads extends Operation {

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
            getOperationService(instance).execute(operation);
        }

        public void await() {
            operation.join();
        }
    }

    private static class SlowRecursiveOperation extends JoinableOperation {

        private final int recursionDepth;
        private final int sleepSeconds;

        SlowRecursiveOperation(int partitionId, int recursionDepth, int sleepSeconds) {
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
