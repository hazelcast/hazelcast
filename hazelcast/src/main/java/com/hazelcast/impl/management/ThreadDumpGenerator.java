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

package com.hazelcast.impl.management;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Constructor;
import java.util.logging.Level;

public class ThreadDumpGenerator {

    protected static final ILogger logger = Logger.getLogger(ThreadDumpGenerator.class.getName());

    private static final String THREAD_DUMP_JAVA6_CLASS_NAME = "ThreadDumpGeneratorJava6";
    private static final Class[] TYPE = new Class[]{ThreadMXBean.class};

    public static ThreadDumpGenerator newInstance() throws Exception {
        return newInstance(ManagementFactory.getThreadMXBean());
    }

    public static ThreadDumpGenerator newInstance(ThreadMXBean bean) throws Exception {
        String p = System.getProperty("java.specification.version");
        // 1.4, 1.5, 1.6 ...
        int v = Integer.parseInt(p.split("\\.")[1]);
        if (v >= 6) {
            String pkg = ThreadDumpGenerator.class.getPackage().getName();
            String className = pkg + "." + THREAD_DUMP_JAVA6_CLASS_NAME;
            Class clazz = ThreadDumpGenerator.class.getClassLoader().loadClass(className);
            Constructor<ThreadDumpGenerator> cons = clazz.getConstructor(TYPE);
            return cons.newInstance(bean);
        } else if (v == 5) {
            return new ThreadDumpGenerator(bean);
        } else {
            throw new UnsupportedOperationException("ThreadDumpGenerator can not run on JVM version: " + p);
        }
    }

    protected final ThreadMXBean threadMxBean;

    ThreadDumpGenerator(ThreadMXBean bean) {
        super();
        this.threadMxBean = bean;
    }

    public final String dumpAllThreads() {
        logger.log(Level.FINEST, "Generating full thread dump...");
        StringBuilder s = new StringBuilder();
        s.append("Full thread dump ");
        return dump(getAllThreads(), s);
    }

    public final String dumpDeadlocks() {
        logger.log(Level.FINEST, "Generating dead-locked threads dump...");
        StringBuilder s = new StringBuilder();
        s.append("Deadlocked thread dump ");
        return dump(findDeadlockedThreads(), s);
    }

    private String dump(ThreadInfo[] infos, StringBuilder s) {
        header(s);
        appendThreadInfos(infos, s);
        if (logger.isLoggable(Level.FINEST)) {
            logger.log(Level.FINEST, "\n" + s.toString());
        }
        return s.toString();
    }

    public ThreadInfo[] getAllThreads() {
        return getThreads(threadMxBean.getAllThreadIds());
    }

    public ThreadInfo[] findDeadlockedThreads() {
        return getThreads(threadMxBean.findMonitorDeadlockedThreads());
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
            s.append(info);
        }
    }

    protected ThreadInfo[] getThreads(long[] tids) {
        if (tids == null || tids.length == 0) return null;
        return threadMxBean.getThreadInfo(tids, Integer.MAX_VALUE);
    }
}
