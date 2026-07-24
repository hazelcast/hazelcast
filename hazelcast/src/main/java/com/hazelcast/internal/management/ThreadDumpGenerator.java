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

package com.hazelcast.internal.management;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

/**
 * Generates thread dumps.
 */
public final class ThreadDumpGenerator {

    private static final ILogger LOGGER = Logger.getLogger(ThreadDumpGenerator.class);

    private static final String THREAD_PRINT_OPERATION = "threadPrint";
    private static final String[] THREAD_PRINT_ARGS = {"-l"};
    private static final Object[] THREAD_PRINT_PARAMS = {THREAD_PRINT_ARGS};
    private static final String[] STRING_ARRAY_SIGNATURE = {String[].class.getName()};
    private static final ObjectName DIAGNOSTIC_COMMAND_MBEAN = createDiagnosticCommandObjectName();

    private ThreadDumpGenerator() {
    }

    private static ObjectName createDiagnosticCommandObjectName() {
        try {
            return new ObjectName("com.sun.management:type=DiagnosticCommand");
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static String dumpAllThreads() {
        LOGGER.finest("Generating full thread dump...");
        String dump = dumpAllThreadsViaDiagnosticCommandOrMxBean();
        LOGGER.finest(dump);
        return dump;
    }

    public static String dumpDeadlocks() {
        LOGGER.finest("Generating dead-locked threads dump...");
        return dump(findDeadlockedThreads(), new StringBuilder("Deadlocked thread dump "));
    }

    private static String dump(ThreadInfo[] infos, StringBuilder s) {
        header(s);
        appendThreadInfos(infos, s);
        LOGGER.finest(s.toString());
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

    private static String dumpAllThreadsViaDiagnosticCommandOrMxBean() {
        try {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            // Equivalent to: jcmd <pid> Thread.print -l
            return (String) mBeanServer.invoke(
                    DIAGNOSTIC_COMMAND_MBEAN, THREAD_PRINT_OPERATION, THREAD_PRINT_PARAMS, STRING_ARRAY_SIGNATURE);
        } catch (Exception e) {
            LOGGER.finest("Failed to generate thread dump via DiagnosticCommand MBean, falling back to ThreadMXBean", e);

            StringBuilder s = new StringBuilder();
            s.append("Full thread dump ");
            return dump(getAllThreads(), s);
        }
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
