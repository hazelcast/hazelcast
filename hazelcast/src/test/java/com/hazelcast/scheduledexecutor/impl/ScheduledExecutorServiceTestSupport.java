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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.scheduledexecutor.AutoDisposableTask;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.scheduledexecutor.NamedTask;
import com.hazelcast.scheduledexecutor.StatefulTask;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_CANCELLED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_COMPLETED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_CREATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_PENDING;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_STARTED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_TOTAL_EXECUTION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_TOTAL_START_LATENCY;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Common methods used in ScheduledExecutorService tests.
 */
public class ScheduledExecutorServiceTestSupport extends HazelcastTestSupport {

    public IScheduledExecutorService getScheduledExecutor(HazelcastInstance[] instances, String name) {
        return instances[0].getScheduledExecutorService(name);
    }

    int getPartitionIdFromPartitionAwareTask(HazelcastInstance instance, PartitionAware task) {
        return instance.getPartitionService().getPartition(task.getPartitionKey()).getPartitionId();
    }

    protected HazelcastInstance[] createClusterWithCount(int count) {
        return createClusterWithCount(count, new Config());
    }

    protected HazelcastInstance[] createClusterWithCount(int count, Config config) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = factory.newInstances(config, count);
        waitAllForSafeState(instances);
        return instances;
    }

    int countScheduledTasksOn(IScheduledExecutorService scheduledExecutorService) {
        Map<Member, List<IScheduledFuture<Double>>> allScheduled = scheduledExecutorService.getAllScheduledFutures();

        int total = 0;
        for (Member member : allScheduled.keySet()) {
            total += allScheduled.get(member).size();
        }

        return total;
    }

    public static class StatefulRunnableTask
            implements Runnable, StatefulTask<String, Integer>, HazelcastInstanceAware, Serializable {

        private static final String STATE_KEY = "status";

        private final String latchName;
        private final String runCounterName;
        private final String loadCounterName;

        private int status;

        private transient HazelcastInstance instance;

        public StatefulRunnableTask(String latchName, String runCounterName, String loadCounterName) {
            this.latchName = latchName;
            this.runCounterName = runCounterName;
            this.loadCounterName = loadCounterName;
        }

        @Override
        public void run() {
            status++;
            signal(instance, runCounterName);
            signal(instance, latchName);
        }

        @Override
        public void load(Map<String, Integer> snapshot) {
            if (snapshot.containsKey(STATE_KEY)) {
                status = snapshot.get(STATE_KEY);
            }
            signal(instance, loadCounterName);
        }

        @Override
        public void save(Map<String, Integer> snapshot) {
            snapshot.put(STATE_KEY, status);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }
    }

    public static class ICountdownLatchCallableTask
            implements Callable<Double>, HazelcastInstanceAware, Serializable {

        private final String initName;
        private final String waitName;
        private final String doneName;
        private transient HazelcastInstance instance;

        public ICountdownLatchCallableTask(String initName, String waitName, String doneName) {
            this.initName = initName;
            this.waitName = waitName;
            this.doneName = doneName;
        }

        @Override
        public Double call() {
            signal(instance, initName);
            awaitGate(instance, waitName);
            signal(instance, doneName);
            return 77 * 2.2;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }
    }

    public static class ICountdownLatchMapIncrementCallableTask
            implements Callable<Double>, HazelcastInstanceAware, Serializable {

        private final String mapName;
        private final String runEntryCounterName;
        private final String startedName;
        private final String finishedName;
        private final String waitAfterStartName;
        private transient HazelcastInstance instance;

        public ICountdownLatchMapIncrementCallableTask(String mapName, String runEntryCounterName,
                                                       String startedName, String finishedName,
                                                       String waitAfterStartName) {
            this.mapName = mapName;
            this.runEntryCounterName = runEntryCounterName;
            this.startedName = startedName;
            this.finishedName = finishedName;
            this.waitAfterStartName = waitAfterStartName;
        }

        @Override
        public Double call() {
            // signalled on every execution -> ends at 2 (original + migrated re-run)
            signal(instance, runEntryCounterName);
            signal(instance, startedName);
            // first run blocks here and is killed by instances[0] shutdown;
            // the migrated re-run passes straight through (gate already open)
            awaitGate(instance, waitAfterStartName);
            // only the completing run reaches here -> "foo" 1 -> 2
            instance.<String, Integer>getMap(mapName).merge("foo", 1, Integer::sum);
            signal(instance, finishedName);
            return 0.0;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }
    }

    public static class ICountdownLatchRunnableTask
            implements Runnable, HazelcastInstanceAware, Serializable {

        private final String[] names;
        private transient HazelcastInstance instance;

        public ICountdownLatchRunnableTask(String... names) {
            this.names = names;
        }

        @Override
        public void run() {
            signal(instance, names);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }
    }

    static class HotLoopBusyTask implements Runnable, HazelcastInstanceAware, Serializable {

        private final String runFinishedLatchName;

        private transient HazelcastInstance instance;

        HotLoopBusyTask(String runFinishedLatchName) {
            this.runFinishedLatchName = runFinishedLatchName;
        }

        @Override
        public void run() {
            long start = currentTimeMillis();
            while (true) {
                try {
                    sleep(5000);
                    if (currentTimeMillis() - start >= 30000) {
                        signal(instance, runFinishedLatchName);
                        break;
                    }
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }
    }

    static class PlainCallableTask implements Callable<Double>, Serializable {

        private int delta = 0;

        PlainCallableTask() {
        }

        PlainCallableTask(int delta) {
            this.delta = delta;
        }

        @Override
        public Double call() throws Exception {
            return calculateResult(delta);
        }

        public static double calculateResult(int delta) {
            return 5 * 5.0 + delta;
        }
    }

    static class PlainRunnableTask implements Runnable, Serializable {

        PlainRunnableTask() {
        }

        @Override
        public void run() {
            System.out.println("PlainRunnableTask");
        }
    }

    static class EchoTask implements Runnable, Serializable {

        EchoTask() {
        }

        @Override
        public void run() {
            System.out.println("Echo ...cho ...oo ..o");
        }

    }

    static class OneSecondSleepingTask implements Runnable, Serializable {

        OneSecondSleepingTask() {
        }

        @Override
        public void run() {
            sleepSeconds(1);
        }

    }

    static class CountableRunTask implements Runnable, Serializable {
        private final CountDownLatch progress;
        private final Semaphore suspend;

        CountableRunTask(CountDownLatch progress, Semaphore suspend) {
            this.progress = progress;
            this.suspend = suspend;
        }

        @Override
        public void run() {
            progress.countDown();

            if (progress.getCount() == 0) {
                try {
                    suspend.acquire();
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
        }

    }

    public static class ErroneousCallableTask
            implements Callable<Double>, HazelcastInstanceAware, Serializable {

        private final String completionName;
        private transient HazelcastInstance instance;

        public ErroneousCallableTask(String completionName) {
            this.completionName = completionName;
        }

        @Override
        public Double call() {
            if (completionName != null) {
                signal(instance, completionName);
            }
            throw new IllegalStateException("Erroneous task");
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }
    }

    static class ErroneousRunnableTask implements Runnable, Serializable {

        @Override
        public void run() {
            throw new IllegalStateException("Erroneous task");
        }

    }


    public static class PlainInstanceAwareRunnableTask
            implements Runnable, HazelcastInstanceAware, Serializable {

        private final String name;
        private transient HazelcastInstance instance;

        public PlainInstanceAwareRunnableTask(String name) {
            this.name = name;
        }

        @Override
        public void run() {
            if (instance == null) {
                throw new IllegalStateException("HazelcastInstance was not injected");
            }
            signal(instance, name);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }
    }

    static class PlainPartitionAwareCallableTask implements Callable<Double>, Serializable, PartitionAware<String> {

        @Override
        public Double call() throws Exception {
            return 5 * 5.0;
        }

        @Override
        public String getPartitionKey() {
            return "TestKey";
        }
    }

    public static class PlainPartitionAwareRunnableTask
            implements Runnable, PartitionAware<String>, HazelcastInstanceAware, Serializable {

        private final String name;
        private transient HazelcastInstance instance;

        public PlainPartitionAwareRunnableTask(String name) {
            this.name = name;
        }

        @Override
        public void run() {
            signal(instance, name);
        }

        @Override
        public String getPartitionKey() {
            return "PartitionAwareRunnableTaskKey";
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }
    }

    public static class HazelcastInstanceAwareRunnable
            implements Callable<Boolean>, HazelcastInstanceAware, Serializable, NamedTask {

        private transient volatile HazelcastInstance instance;
        private final String name;

        HazelcastInstanceAwareRunnable(String name) {
            this.name = name;
        }

        @Override
        public void setHazelcastInstance(final HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Boolean call() {
            return (instance != null);
        }
    }

    public static class AutoDisposableCallable implements Callable<Boolean>, AutoDisposableTask {

        @Override
        public Boolean call() {
            return true;
        }
    }

    public static class AutoDisposableRunnable implements Runnable, AutoDisposableTask {

        @Override
        public void run() {
        }
    }

    public static class NamedCallable implements Callable<Boolean>, NamedTask, Serializable {

        public static final String NAME = "NAMED-CALLABLE";

        @Override
        public Boolean call() {
            return true;
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

    public static class NamedRunnable implements Runnable, NamedTask, Serializable {

        public static final String NAME = "NAMED-RUNNABLE";

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public void run() {

        }
    }

    public static class AllTasksRunningWithinNumOfNodes implements AssertTask {

        private final IScheduledExecutorService scheduler;
        private final int expectedNodesWithTasks;

        AllTasksRunningWithinNumOfNodes(IScheduledExecutorService scheduler, int expectedNodesWithTasks) {
            this.scheduler = scheduler;
            this.expectedNodesWithTasks = expectedNodesWithTasks;
        }

        @Override
        public void run() throws Exception {

            int actualNumOfNodesWithTasks = 0;
            Map<Member, List<IScheduledFuture<Object>>> allScheduledFutures = scheduler.getAllScheduledFutures();
            for (Member member : allScheduledFutures.keySet()) {
                if (!allScheduledFutures.get(member).isEmpty()) {
                    actualNumOfNodesWithTasks++;
                }
            }
            if (actualNumOfNodesWithTasks != expectedNodesWithTasks) {
                throw new IllegalStateException("Actual nodes with tasks: " + actualNumOfNodesWithTasks + ". "
                        + "Expected: " + expectedNodesWithTasks);
            }

            for (List<IScheduledFuture<Object>> futures : allScheduledFutures.values()) {
                for (IScheduledFuture future : futures) {
                    if (future.isCancelled()) {
                        throw new IllegalStateException("Scheduled task: " + future.getHandler().getTaskName()
                                + " is cancelled.");
                    } else if (future.getStats().getTotalRuns() == 0) {
                        throw new AssertionError();
                    }
                }
            }
        }
    }

    public static Map<String, List<Long>> collectMetrics(String prefix, HazelcastInstance... instances) {
        Map<String, List<Long>> metricsMap = new HashMap<>();

        for (HazelcastInstance instance : instances) {
            Accessors.getMetricsRegistry(instance).collect(new MetricsCollector() {
                @Override
                public void collectLong(MetricDescriptor descriptor, long value) {
                    if (prefix.equals(descriptor.prefix())) {
                        metricsMap.compute(descriptor.metric(), (metricName, values) -> {
                            if (values == null) {
                                values = new ArrayList<>();
                            }
                            values.add(value);
                            return values;
                        });
                    }
                }

                @Override
                public void collectDouble(MetricDescriptor descriptor, double value) {
                }

                @Override
                public void collectException(MetricDescriptor descriptor, Exception e) {
                }

                @Override
                public void collectNoValue(MetricDescriptor descriptor) {
                }
            });
        }
        return metricsMap;
    }

    public static void assertMetricsCollected(Map<String, List<Long>> metricsMap,
                                              long expectedTotalExecutionTime,
                                              long expectedPending,
                                              long expectedStarted,
                                              long expectedCompleted,
                                              long expectedCancelled,
                                              long expectedCreationTime,
                                              long expectedTotalStartLatency) {

        List<Long> totalExecutionTimes = metricsMap.get(EXECUTOR_METRIC_TOTAL_EXECUTION_TIME);
        for (long totalExecutionTime : totalExecutionTimes) {
            assertGreaterOrEquals(EXECUTOR_METRIC_TOTAL_EXECUTION_TIME + "::" + metricsMap,
                    totalExecutionTime, expectedTotalExecutionTime);
        }

        List<Long> pendingCount = metricsMap.get(EXECUTOR_METRIC_PENDING);
        for (long pending : pendingCount) {
            assertEquals(EXECUTOR_METRIC_PENDING, expectedPending, pending);
        }

        List<Long> startedCount = metricsMap.get(EXECUTOR_METRIC_STARTED);
        for (long started : startedCount) {
            assertEquals(EXECUTOR_METRIC_STARTED, expectedStarted, started);
        }

        List<Long> completedCount = metricsMap.get(EXECUTOR_METRIC_COMPLETED);
        for (long completed : completedCount) {
            assertEquals(EXECUTOR_METRIC_COMPLETED, expectedCompleted, completed);
        }

        List<Long> cancelledCount = metricsMap.get(EXECUTOR_METRIC_CANCELLED);
        for (long cancelled : cancelledCount) {
            assertEquals(EXECUTOR_METRIC_CANCELLED, expectedCancelled, cancelled);
        }

        List<Long> creationTimes = metricsMap.get(EXECUTOR_METRIC_CREATION_TIME);
        for (long creationTime : creationTimes) {
            assertGreaterOrEquals(EXECUTOR_METRIC_CREATION_TIME,
                    creationTime, expectedCreationTime);
        }

        List<Long> totalStartLatencies = metricsMap.get(EXECUTOR_METRIC_TOTAL_START_LATENCY);
        for (long totalStartLatency : totalStartLatencies) {
            assertGreaterOrEquals(EXECUTOR_METRIC_TOTAL_START_LATENCY,
                    totalStartLatency, expectedTotalStartLatency);
        }
    }


    protected static final String COORDINATION_MAP = "coordination";

    protected static void signal(HazelcastInstance hz, String... names) {
        for (String name : names) {
            hz.<String, Long>getMap(COORDINATION_MAP).merge(name, 1L, Long::sum);
        }
    }

    protected static long signalCount(HazelcastInstance hz, String name) {
        Long value = hz.<String, Long>getMap(COORDINATION_MAP).get(name);
        return value == null ? 0L : value;
    }

    protected void assertSignalledEventually(HazelcastInstance hz, String name, long expected) {
        assertTrueEventually(() -> assertTrue(
                "expected signal count >= " + expected + " for '" + name + "' but was " + signalCount(hz, name),
                signalCount(hz, name) >= expected));
    }

    protected static void awaitGate(HazelcastInstance hz, String name) {
        IMap<String, Long> map = hz.getMap(COORDINATION_MAP);
        while (true) {
            Long value = map.get(name);
            if (value != null && value >= 1L) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
