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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.scheduledexecutor.AutoDisposableTask;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.scheduledexecutor.NamedTask;
import com.hazelcast.scheduledexecutor.StatefulTask;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;

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

    static class StatefulRunnableTask implements Runnable, Serializable, HazelcastInstanceAware, StatefulTask<String, Integer> {

        final String latchName;
        final String runCounterName;
        final String loadCounterName;

        int status = 0;

        transient HazelcastInstance instance;

        StatefulRunnableTask(String runsCountLatchName, String runCounterName, String loadCounterName) {
            this.latchName = runsCountLatchName;
            this.runCounterName = runCounterName;
            this.loadCounterName = loadCounterName;
        }

        @Override
        public void run() {
            status++;
            instance.getCPSubsystem().getAtomicLong(runCounterName).set(status);
            instance.getCPSubsystem().getCountDownLatch(latchName).countDown();
        }

        @Override
        public void load(Map<String, Integer> snapshot) {
            status = snapshot.get("status");
            instance.getCPSubsystem().getAtomicLong(loadCounterName).incrementAndGet();
        }

        @Override
        public void save(Map<String, Integer> snapshot) {
            snapshot.put("status", status);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }
    }

    static class ICountdownLatchCallableTask implements Callable<Double>, Serializable, HazelcastInstanceAware {

        final String initLatchName;
        final String waitLatchName;
        final String doneLatchName;

        transient HazelcastInstance instance;

        ICountdownLatchCallableTask(String initLatchName, String waitLatchName, String doneLatchName) {
            this.initLatchName = initLatchName;
            this.waitLatchName = waitLatchName;
            this.doneLatchName = doneLatchName;
        }

        @Override
        public Double call() {
            instance.getCPSubsystem().getCountDownLatch(initLatchName).countDown();
            assertOpenEventually(instance.getCPSubsystem().getCountDownLatch(waitLatchName));
            instance.getCPSubsystem().getCountDownLatch(doneLatchName).countDown();
            return 77 * 2.2;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }
    }

    static class ICountdownLatchMapIncrementCallableTask implements Runnable, Serializable, HazelcastInstanceAware {

        final String startedLatch;
        final String finishedLatch;
        final String waitAfterStartLatch;
        final String runEntryCounterName;
        final String mapName;

        transient HazelcastInstance instance;

        ICountdownLatchMapIncrementCallableTask(String mapName, String runEntryCounterName,
                                                String startedLatch, String finishedLatch, String waitAfterStartLatch) {
            this.mapName = mapName;
            this.runEntryCounterName = runEntryCounterName;
            this.startedLatch = startedLatch;
            this.finishedLatch = finishedLatch;
            this.waitAfterStartLatch = waitAfterStartLatch;
        }

        @Override
        public void run() {
            instance.getCPSubsystem().getAtomicLong(runEntryCounterName).incrementAndGet();
            instance.getCPSubsystem().getCountDownLatch(startedLatch).countDown();
            try {
                instance.getCPSubsystem().getCountDownLatch(waitAfterStartLatch).await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            IMap<String, Integer> map = instance.getMap(mapName);
            if (map.get("foo") == 1) {
                map.put("foo", 2);
            }

            instance.getCPSubsystem().getCountDownLatch(finishedLatch).countDown();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }
    }

    static class ICountdownLatchRunnableTask implements Runnable, Serializable, HazelcastInstanceAware {

        final String[] runsCountDownLatchNames;

        transient HazelcastInstance instance;

        ICountdownLatchRunnableTask(String... runsCountDownLatchNames) {
            this.runsCountDownLatchNames = runsCountDownLatchNames;
        }

        @Override
        public void run() {
            for (String runsCounterLatchName : runsCountDownLatchNames) {
                instance.getCPSubsystem().getCountDownLatch(runsCounterLatchName).countDown();
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
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
                        instance.getCPSubsystem().getCountDownLatch(runFinishedLatchName).countDown();
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

    static class ErroneousCallableTask implements Callable<Double>, Serializable, HazelcastInstanceAware {

        private String completionLatchName;

        private transient HazelcastInstance instance;

        ErroneousCallableTask() {
        }

        ErroneousCallableTask(String completionLatchName) {
            this.completionLatchName = completionLatchName;
        }

        @Override
        public Double call() throws Exception {
            try {
                throw new IllegalStateException("Erroneous task");
            } finally {
                if (completionLatchName != null) {
                    instance.getCPSubsystem().getCountDownLatch(completionLatchName).countDown();
                }
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }
    }

    static class ErroneousRunnableTask implements Runnable, Serializable {

        @Override
        public void run() {
            throw new IllegalStateException("Erroneous task");
        }

    }


    static class PlainInstanceAwareRunnableTask implements Runnable, Serializable, HazelcastInstanceAware {

        private final String latchName;

        private transient HazelcastInstance instance;

        PlainInstanceAwareRunnableTask(String latchName) {
            this.latchName = latchName;
        }

        @Override
        public void run() {
            this.instance.getCPSubsystem().getCountDownLatch(latchName).countDown();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            instance = hazelcastInstance;
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

    static class PlainPartitionAwareRunnableTask implements Runnable, Serializable, PartitionAware<String>, HazelcastInstanceAware {

        private final String latchName;

        private transient HazelcastInstance instance;

        PlainPartitionAwareRunnableTask(String latchName) {
            this.latchName = latchName;
        }

        @Override
        public void run() {
            this.instance.getCPSubsystem().getCountDownLatch(latchName).countDown();
        }

        @Override
        public String getPartitionKey() {
            return "TestKey";
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
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
}
