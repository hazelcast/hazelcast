/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;

/**
 * Common methods used in ScheduledExecutorService tests
 */
public class ScheduledExecutorServiceTestSupport extends HazelcastTestSupport {

    public IScheduledExecutorService getScheduledExecutor(HazelcastInstance[] instances, String name) {
        return instances[0].getScheduledExecutorService(name);
    }

    public int getPartitionIdFromPartitionAwareTask(HazelcastInstance instance, PartitionAware task) {
        return instance.getPartitionService().getPartition(task.getPartitionKey()).getPartitionId();
    }

    public HazelcastInstance[] createClusterWithCount(int count) {
        return createClusterWithCount(count, new Config());
    }

    public HazelcastInstance[] createClusterWithCount(int count, Config config) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = factory.newInstances(config, count);
        waitAllForSafeState(instances);
        return instances;
    }

    public int countScheduledTasksOn(IScheduledExecutorService scheduledExecutorService) {
        Map<Member, List<IScheduledFuture<Double>>> allScheduled = scheduledExecutorService.getAllScheduledFutures();

        int total = 0;
        for (Member member : allScheduled.keySet()) {
            total += allScheduled.get(member).size();
        }

        return total;
    }

    static class StatefulRunnableTask
            implements Runnable, Serializable, HazelcastInstanceAware, StatefulTask<String, Integer> {

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
            instance.getAtomicLong(runCounterName).set(status);
            instance.getCountDownLatch(latchName).countDown();
        }

        @Override
        public void load(Map<String, Integer> snapshot) {
            status = snapshot.get("status");
            instance.getAtomicLong(loadCounterName).incrementAndGet();
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

    static class ICountdownLatchCallableTask
            implements Callable<Double>, Serializable, HazelcastInstanceAware {

        final String runLatchName;

        final int sleepPeriod;

        transient HazelcastInstance instance;

        ICountdownLatchCallableTask(String runLatchName,
                                    int sleepPeriod) {
            this.runLatchName = runLatchName;
            this.sleepPeriod = sleepPeriod;
        }

        @Override
        public Double call() {
            try {
                sleep(sleepPeriod);
            } catch (InterruptedException e) {
                Thread.interrupted();
            }

            instance.getCountDownLatch(runLatchName).countDown();
            return 77 * 2.2;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }
    }

    static class ICountdownLatchMapIncrementCallableTask
            implements Runnable, Serializable, HazelcastInstanceAware {

        final String runLatchName;

        final String runEntryCounterName;

        final String mapName;

        transient HazelcastInstance instance;

        ICountdownLatchMapIncrementCallableTask(String mapName, String runEntryCounterName,
                                                String runLatchName) {
            this.mapName = mapName;
            this.runEntryCounterName = runEntryCounterName;
            this.runLatchName = runLatchName;
        }

        @Override
        public void run() {
            instance.getAtomicLong(runEntryCounterName).incrementAndGet();

            IMap<String, Integer> map = instance.getMap(mapName);
            for (int i = 0; i < 100000; i++) {
                if (map.get(String.valueOf(i)) == i) {
                    map.put(String.valueOf(i), i + 1);
                }
            }

            instance.getCountDownLatch(runLatchName).countDown();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }
    }

    static class ICountdownLatchRunnableTask
            implements Runnable, Serializable, HazelcastInstanceAware {

        final String[] runsCounterlatchNames;

        transient HazelcastInstance instance;

        ICountdownLatchRunnableTask(String... runsCounterlatchNames) {
            this.runsCounterlatchNames = runsCounterlatchNames;
        }

        @Override
        public void run() {
            for (String runsCounterLatchName : runsCounterlatchNames) {
                instance.getCountDownLatch(runsCounterLatchName).countDown();
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }
    }

    static class HotLoopBusyTask
            implements Runnable, HazelcastInstanceAware, Serializable {

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
                        instance.getCountDownLatch(runFinishedLatchName).countDown();
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

    static class PlainCallableTask
            implements Callable<Double>, Serializable {

        private int delta = 0;

        public PlainCallableTask() {
        }

        public PlainCallableTask(int delta) {
            this.delta = delta;
        }

        @Override
        public Double call()
                throws Exception {
            return 5 * 5.0 + delta;
        }

    }

    static class EchoTask implements Runnable, Serializable {

        public EchoTask() {
        }

        @Override
        public void run() {
            System.out.println("Echo ...cho ...oo ..o");
        }

    }

    static class ErroneousCallableTask
            implements Callable<Double>, Serializable, HazelcastInstanceAware {

        private String completionLatchName;

        private transient HazelcastInstance instance;

        ErroneousCallableTask() {
        }

        ErroneousCallableTask(String completionLatchName) {
            this.completionLatchName = completionLatchName;
        }

        @Override
        public Double call()
                throws Exception {
            try {
                throw new IllegalStateException("Erroneous task");
            } finally {
                if (completionLatchName != null) {
                    instance.getCountDownLatch(completionLatchName).countDown();
                }
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.instance = hazelcastInstance;
        }
    }

    static class PlainPartitionAwareCallableTask
            implements Callable<Double>, Serializable, PartitionAware<String> {

        @Override
        public Double call()
                throws Exception {
            return 5 * 5.0;
        }

        @Override
        public String getPartitionKey() {
            return "TestKey";
        }
    }

    static class PlainPartitionAwareRunnableTask
            implements Runnable, Serializable, PartitionAware<String>, HazelcastInstanceAware {

        private final String latchName;

        private transient HazelcastInstance instance;

        public PlainPartitionAwareRunnableTask(String latchName) {
            this.latchName = latchName;
        }

        @Override
        public void run() {
            this.instance.getCountDownLatch(latchName).countDown();
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

        public HazelcastInstanceAwareRunnable(String name) {
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
}
