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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

/**
 * Generates thread dumps.
 */
public final class ThreadDumpGenerator {

    private static final ILogger LOGGER = Logger.getLogger(ThreadDumpGenerator.class);

    private ThreadDumpGenerator() {
    }

    public static String dumpAllThreads() {
        LOGGER.finest("Generating full thread dump...");
        StringBuilder s = new StringBuilder();
        s.append("Full thread dump ");
        return dump(getAllThreads(), s);
    }

    public static String dumpDeadlocks() {
        LOGGER.finest("Generating dead-locked threads dump...");
        StringBuilder s = new StringBuilder();
        s.append("Deadlocked thread dump ");
        return dump(findDeadlockedThreads(), s);
    }

    private static String dump(ThreadInfo[] infos, StringBuilder s) {
        header(s);
        appendThreadInfos(infos, s);
        if (LOGGER.isFinestEnabled()) {
            LOGGER.finest("\n" + s.toString());
        }
        return s.toString();
    }

    public static ThreadInfo[] findDeadlockedThreads() {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        if (threadMXBean.isSynchronizerUsageSupported()) {
            long[] deadlockedThreads = threadMXBean.findDeadlockedThreads();
            if (deadlockedThreads == null || deadlockedThreads.length == 0) {
                return null;
            }
            return threadMXBean.getThreadInfo(deadlockedThreads, true, true);
        } else {
            long[] monitorDeadlockedThreads = threadMXBean.findMonitorDeadlockedThreads();
            return getThreadInfos(threadMXBean, monitorDeadlockedThreads);
        }
    }

    public static ThreadInfo[] getAllThreads() {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        if (threadMXBean.isObjectMonitorUsageSupported()
                && threadMXBean.isSynchronizerUsageSupported()) {
            return threadMXBean.dumpAllThreads(true, true);
        }
        long[] allThreadIds = threadMXBean.getAllThreadIds();
        return getThreadInfos(threadMXBean, allThreadIds);
    }

    private static ThreadInfo[] getThreadInfos(ThreadMXBean threadMXBean, long[] threadIds) {
        if (threadIds == null || threadIds.length == 0) {
            return null;
        }
        return threadMXBean.getThreadInfo(threadIds, Integer.MAX_VALUE);
    }

    private static void header(StringBuilder s) {
        s.append(System.getProperty("java.vm.name"));
        s.append(" (");
        s.append(System.getProperty("java.vm.version"));
        s.append(" ");
        s.append(System.getProperty("java.vm.info"));
        s.append("):");
        s.append("\n\n");
    }

    private static void appendThreadInfos(ThreadInfo[] infos, StringBuilder s) {
        if (infos == null || infos.length == 0) {
            return;
        }
        for (ThreadInfo info : infos) {
            s.append(info);
        }
    }
}
