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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ThreadMonitoringService {

    final ConcurrentMap<Long, ThreadCpuInfo> knownThreads = new ConcurrentHashMap<Long, ThreadCpuInfo>(100);
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    private final ThreadGroup threadGroup;

    public ThreadMonitoringService(ThreadGroup threadGroup) {
        this.threadGroup = threadGroup;
    }

    class ThreadCpuInfo {
        final Thread thread;
        long lastSet = 0;
        long lastValue = 0;

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
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ThreadCpuInfo that = (ThreadCpuInfo) o;
            if (thread.getId() != that.thread.getId()) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return thread.hashCode();
        }

        @Override
        public String toString() {
            return "ThreadCpuInfo{" +
                    "name='" + thread.getName() + '\'' +
                    ", threadId=" + thread.getId() +
                    ", lastSet=" + lastSet +
                    ", lastValue=" + lastValue +
                    '}';
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
                ThreadCpuInfo t = knownThreads.get(thread.getId());
                if (t == null) {
                    t = new ThreadCpuInfo(thread);
                    knownThreads.putIfAbsent(thread.getId(), t);
                }
                int percentage = (int) ((t.setNewValue(threadMXBean.getThreadCpuTime(thread.getId()), now)) * 100);
                monitoredThreads.add(new MonitoredThread(thread.getName(), thread.getId(), percentage));
            }
            return monitoredThreads;
        } catch (Exception e) {
            return null;
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
