/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

/**
 * ThreadDump Java 1.5 implementation
 */

class ThreadDumpGeneratorImpl_15 extends ThreadDumpGenerator {

    public ThreadDumpGeneratorImpl_15(ThreadMXBean bean) {
        super(bean);
    }

    /**
     * copied from JDK 1.6 {@link ThreadInfo} toString()
     */
    protected void appendThreadInfo(ThreadInfo info, StringBuilder sb) {
        sb.append("\"").append(info.getThreadName()).append("\"")
                .append(" Id=").append(info.getThreadId()).append(" ").append(info.getThreadState());
        if (info.getLockName() != null) {
            sb.append(" on ").append(info.getLockName());
        }
        if (info.getLockOwnerName() != null) {
            sb.append(" owned by \"")
                    .append(info.getLockOwnerName())
                    .append("\" Id=").append(info.getLockOwnerId());
        }
        if (info.isSuspended()) {
            sb.append(" (suspended)");
        }
        if (info.isInNative()) {
            sb.append(" (in native)");
        }
        sb.append('\n');
        StackTraceElement[] stackTrace = info.getStackTrace();
        for (int i = 0; i < stackTrace.length; i++) {
            StackTraceElement ste = stackTrace[i];
            sb.append("\tat ").append(ste.toString());
            sb.append('\n');
        }
        sb.append('\n');
    }
}
