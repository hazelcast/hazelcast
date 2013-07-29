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

import java.lang.management.LockInfo;
import java.lang.management.*;
import java.util.logging.Level;

public class ThreadDumpGenerator {

    protected static final ILogger logger = Logger.getLogger(ThreadDumpGenerator.class);

    public static ThreadDumpGenerator newInstance() {
        return newInstance(ManagementFactory.getThreadMXBean());
    }

    public static ThreadDumpGenerator newInstance(ThreadMXBean bean) {
        return new ThreadDumpGenerator(bean);
    }

    protected final ThreadMXBean threadMxBean;

    ThreadDumpGenerator(ThreadMXBean bean) {
        super();
        this.threadMxBean = bean;
    }

    public final String dumpAllThreads() {
        logger.finest( "Generating full thread dump...");
        StringBuilder s = new StringBuilder();
        s.append("Full thread dump ");
        return dump(getAllThreads(), s);
    }

    public final String dumpDeadlocks() {
        logger.finest( "Generating dead-locked threads dump...");
        StringBuilder s = new StringBuilder();
        s.append("Deadlocked thread dump ");
        return dump(findDeadlockedThreads(), s);
    }

    private String dump(ThreadInfo[] infos, StringBuilder s) {
        header(s);
        appendThreadInfos(infos, s);
        if (logger.isFinestEnabled()) {
            logger.finest( "\n" + s.toString());
        }
        return s.toString();
    }

    public ThreadInfo[] findDeadlockedThreads() {
        if (threadMxBean.isSynchronizerUsageSupported()) {
            long[] tids = threadMxBean.findDeadlockedThreads();
            if (tids == null || tids.length == 0) {
                return null;
            }
            return threadMxBean.getThreadInfo(tids, true, true);
        } else {
            return getThreads(threadMxBean.findMonitorDeadlockedThreads());
        }
    }

    public ThreadInfo[] getAllThreads() {
        if (threadMxBean.isObjectMonitorUsageSupported()
                && threadMxBean.isSynchronizerUsageSupported()) {
            return threadMxBean.dumpAllThreads(true, true);
        }
        return getThreads(threadMxBean.getAllThreadIds());
    }

    private void header(StringBuilder s) {
        s.append(System.getProperty("java.vm.name"));
        s.append(" (");
        s.append(System.getProperty("java.vm.version"));
        s.append(" ");
        s.append(System.getProperty("java.vm.info"));
        s.append("):");
        s.append("\n\n");
    }

    private void appendThreadInfos(ThreadInfo[] infos, StringBuilder s) {
        if (infos == null || infos.length == 0) return;
        for (int i = 0; i < infos.length; i++) {
            ThreadInfo info = infos[i];
            appendThreadInfo(info, s);
        }
    }

    protected void appendThreadInfo(ThreadInfo info, StringBuilder sb) {
        sb.append("\"").append(info.getThreadName()).append("\"").append(
                " Id=").append(info.getThreadId()).append(" ").append(
                info.getThreadState());
        if (info.getLockName() != null) {
            sb.append(" on ").append(info.getLockName());
        }
        if (info.getLockOwnerName() != null) {
            sb.append(" owned by \"").append(info.getLockOwnerName()).
                    append("\" Id=").append(+info.getLockOwnerId());
        }
        if (info.isSuspended()) {
            sb.append(" (suspended)");
        }
        if (info.isInNative()) {
            sb.append(" (in native)");
        }
        sb.append('\n');
        final StackTraceElement[] stackTrace = info.getStackTrace();
        final Object lockInfo = info.getLockInfo();
        final MonitorInfo[] monitorInfo = info.getLockedMonitors();
        for (int i = 0; i < stackTrace.length; i++) {
            StackTraceElement ste = stackTrace[i];
            sb.append("\tat ").append(ste.toString());
            sb.append('\n');
            if (i == 0 && lockInfo != null) {
                Thread.State ts = info.getThreadState();
                switch (ts) {
                    case BLOCKED:
                        sb.append("\t-  blocked on ").append(lockInfo);
                        sb.append('\n');
                        break;
                    case WAITING:
                        sb.append("\t-  waiting on ").append(lockInfo);
                        sb.append('\n');
                        break;
                    case TIMED_WAITING:
                        sb.append("\t-  waiting on ").append(lockInfo);
                        sb.append('\n');
                        break;
                    default:
                }
            }
            for (MonitorInfo mi : monitorInfo) {
                int depth = mi.getLockedStackDepth();
                if (depth == i) {
                    sb.append("\t-  locked ").append(mi);
                    sb.append('\n');
                }
            }
        }
        final LockInfo[] locks = info.getLockedSynchronizers();
        if (locks.length > 0) {
            sb.append("\n\tNumber of locked synchronizers = ").append(locks.length);
            sb.append('\n');
            for (LockInfo li : locks) {
                sb.append("\t- ").append(li);
                sb.append('\n');
            }
        }
        sb.append('\n');
    }

    protected ThreadInfo[] getThreads(long[] tids) {
        if (tids == null || tids.length == 0) return null;
        return threadMxBean.getThreadInfo(tids, Integer.MAX_VALUE);
    }
}
