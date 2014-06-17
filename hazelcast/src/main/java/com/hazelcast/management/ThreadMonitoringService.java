/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.management;

import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Service for monitoring threads.
 */
public class ThreadMonitoringService {

    private static final int PERCENT_MULTIPLIER = 100;
    final ConcurrentMap<Long, ThreadCpuInfo> knownThreads = new ConcurrentHashMap<Long, ThreadCpuInfo>(100);
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    private final ThreadGroup threadGroup;


    public ThreadMonitoringService(ThreadGroup threadGroup) {
        this.threadGroup = threadGroup;
    }

    /**
     * Holder class for {@link java.lang.Thread} with CPU time.
     */
    static class ThreadCpuInfo {
        final Thread thread;
        long lastSet;
        long lastValue;

        ThreadCpuInfo(Thread thread) {
            this.thread = thread;
        }

        public double setNewValue(long newValue, long now) {
            double diff = newValue - lastValue;
            double timeDiff = now - lastSet;
            lastSet = now;
            lastValue = newValue;
            return diff / timeDiff;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ThreadCpuInfo that = (ThreadCpuInfo) o;
            return thread.getId() == that.thread.getId();
        }

        @Override
        public int hashCode() {
            return thread.hashCode();
        }

        @Override
        public String toString() {
            return "ThreadCpuInfo{"
                    + "name='" + thread.getName() + '\''
                    + ", threadId=" + thread.getId()
                    + ", lastSet=" + lastSet
                    + ", lastValue=" + lastValue
                    + '}';
        }
    }

    public Set<MonitoredThread> getStats() {
        try {
            Set<MonitoredThread> monitoredThreads = new TreeSet<MonitoredThread>();
            int count = threadGroup.activeCount();
            Thread[] threads = new Thread[count];
            threadGroup.enumerate(threads);
            long now = System.nanoTime();
            for (Thread thread : threads) {
                ThreadCpuInfoConstructor constructor = new ThreadCpuInfoConstructor(thread);
                final ThreadCpuInfo t = ConcurrencyUtil.getOrPutIfAbsent(knownThreads, thread.getId(), constructor);
                int percentage = (int) ((t.setNewValue(threadMXBean.getThreadCpuTime(thread.getId()), now)) * PERCENT_MULTIPLIER);
                monitoredThreads.add(new MonitoredThread(thread.getName(), thread.getId(), percentage));
            }
            return monitoredThreads;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Constructor class for {@link com.hazelcast.management.ThreadMonitoringService.ThreadCpuInfo}
     */
    private static final class ThreadCpuInfoConstructor implements ConstructorFunction<Long, ThreadCpuInfo> {

        private Thread thread;

        private ThreadCpuInfoConstructor(Thread thread) {
            this.thread = thread;
        }

        @Override
        public ThreadCpuInfo createNew(Long notUsed) {
            return new ThreadCpuInfo(thread);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ThreadStats {\n");
        final Set<MonitoredThread> stats = getStats();
        if (stats != null) {
            int total = 0;
            for (MonitoredThread monitoredThread : stats) {
                sb.append(monitoredThread.toString());
                sb.append("\n");
                total += monitoredThread.cpuPercentage;
            }
            sb.append("Total::: ").append(total).append('\n');
        }
        sb.append("}");
        return sb.toString();
    }
}
