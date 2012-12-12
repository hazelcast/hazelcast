/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

/**
 * ThreadDump Java 1.6 implementation
 */

class ThreadDumpGeneratorImpl_16 extends ThreadDumpGenerator {

    private static final String ThreadMXBean_isObjectMonitorUsageSupported = "isObjectMonitorUsageSupported";
    private static final String ThreadMXBean_isSynchronizerUsageSupported = "isSynchronizerUsageSupported";
    private static final String ThreadMXBean_dumpAllThreads = "dumpAllThreads";
    private static final String ThreadMXBean_getThreadInfo = "getThreadInfo";
    private static final String ThreadMXBean_findDeadlockedThreads = "findDeadlockedThreads";
    private static final String ThreadInfo_getLockInfo = "getLockInfo";
    private static final String ThreadInfo_getLockedSynchronizers = "getLockedSynchronizers";
    private static final String ThreadInfo_getLockedMonitors = "getLockedMonitors";
    private static final String MonitorInfo_getLockedStackDepth = "getLockedStackDepth";

    private final static ConcurrentMap<String, Method> methods = new ConcurrentHashMap<String, Method>();

    public ThreadDumpGeneratorImpl_16(ThreadMXBean bean) {
        super(bean);
    }

    public ThreadInfo[] getAllThreads() {
        if (booleanCall(threadMxBean, ThreadMXBean_isObjectMonitorUsageSupported)
                && booleanCall(threadMxBean, ThreadMXBean_isSynchronizerUsageSupported)) {
            return parameterizedObjectCall(threadMxBean, ThreadMXBean_dumpAllThreads,
                    new Class[]{boolean.class, boolean.class}, new Object[]{true, true});
        } else {
            return super.getAllThreads();
        }
    }

    public ThreadInfo[] findDeadlockedThreads() {
        if (booleanCall(threadMxBean, ThreadMXBean_isSynchronizerUsageSupported)) {
            long[] tids = objectCall(threadMxBean, ThreadMXBean_findDeadlockedThreads);
            if (tids == null || tids.length == 0) {
                return null;
            }
            return parameterizedObjectCall(threadMxBean, ThreadMXBean_getThreadInfo,
                    new Class[]{long[].class, boolean.class, boolean.class}, new Object[]{tids, true, true});
        } else {
            return super.findDeadlockedThreads();
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
        final Object lockInfo = objectCall(info, ThreadInfo_getLockInfo);
        final Object[] monitorInfo = objectCall(info, ThreadInfo_getLockedMonitors);
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
            for (Object mi : monitorInfo) {
                Integer depth = objectCall(mi, MonitorInfo_getLockedStackDepth);
                if (depth == i) {
                    sb.append("\t-  locked ").append(mi);
                    sb.append('\n');
                }
            }
        }
        final Object[] locks = objectCall(info, ThreadInfo_getLockedSynchronizers);
        if (locks.length > 0) {
            sb.append("\n\tNumber of locked synchronizers = ").append(locks.length);
            sb.append('\n');
            for (Object li : locks) {
                sb.append("\t- ").append(li);
                sb.append('\n');
            }
        }
        sb.append('\n');
    }

    private static boolean booleanCall(Object object, String methodName) {
        Boolean result = objectCall(object, methodName);
        return result != null ? result.booleanValue() : false;
    }

    private static <T extends Object> T objectCall(Object object, String methodName) {
        return (T) parameterizedObjectCall(object, methodName, null, null);
    }

    private static <T extends Object> T parameterizedObjectCall(Object object, String methodName, Class[] types, Object[] params) {
        if (object == null) {
            throw new NullPointerException("Object is mandatory!");
        }
        try {
            Class clazz = object.getClass();
            String mKey = clazz.getName() + "." + methodName;
            Method m = methods.get(mKey);
            if (m == null) {
                m = object.getClass().getMethod(methodName, types);
                m.setAccessible(true);
                final Method anotherMethod = methods.putIfAbsent(mKey, m);
                if (anotherMethod != null) {
                    m = anotherMethod;
                }
            }
            return (T) m.invoke(object, params);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "While invoking method[" +
                    methodName + "] of class[" + object.getClass().getName() + "]", e);
        }
        return null;
    }
}
