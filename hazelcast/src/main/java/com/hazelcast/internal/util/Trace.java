/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.Callable;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class Trace {

    private static final int MIN_LATENCY_US = 200;

    private static final ThreadLocal<Trace> THREAD_LOCAL = new ThreadLocal<Trace>() {
        @Override
        protected Trace initialValue() {
            return new Trace(null);
        }
    };
    private static final int MIN_DURATION = 10;

    private final boolean enabled;
    private final long startNs = nanoTime();
    private long endNs;
    private String text;
    private List<Trace> subTraces = new Stack<Trace>();
    private Trace currentSubTrace;

    // private final Address address;
    private Exception exception;
    private String failureMessage;
    private Stack<Trace> stack = new Stack<Trace>();
    private Map<String, Object> meta;
    private Trace parentTrace;

    public Trace(String text) {
        this.enabled = text != null;
        this.text = text;
        currentSubTrace = this;
    }

    public void complete() {
        if (!enabled) {
            return;
        }

        endNs = nanoTime();
    }

    public void addMeta(String key, Object value) {
        if (!enabled) {
            return;
        }
        if (meta == null) {
            meta = new HashMap<String, Object>();
        }
        meta.put(key, value);
    }

    public void startSubtrace(String id) {
        if (!enabled) {
            return;
        }

        Trace parent = currentSubTrace;
        currentSubTrace = new Trace(id);
        currentSubTrace.parentTrace = parent;
        parent.subTraces.add(currentSubTrace);
        stack.push(currentSubTrace);
    }

    public void completeSubtrace() {
        completeSubtrace(null, null);
    }

    public void completeSubtrace(String failureMessage, Exception e) {
        if (!enabled) {
            return;
        }

        currentSubTrace.endNs = nanoTime();
        currentSubTrace.exception = e;
        currentSubTrace.failureMessage = failureMessage;
        currentSubTrace = currentSubTrace.parentTrace;
    }

    public void subtrace(String id, Runnable task) {
//        if (!enabled) {
//            return;
//        }

        Trace parent = currentSubTrace;
        Trace current = new Trace(id);
        currentSubTrace = current;
        currentSubTrace.parentTrace = parent;
        parent.subTraces.add(currentSubTrace);
//        stack.push(currentSubTrace);
//
//        Trace trace = currentSubTrace;
//        trace.start(id);
        try {
            task.run();
        } finally {
            //          Trace entry = current.stack.pop();
            current.endNs = nanoTime();
            currentSubTrace = current.parentTrace;
        }
    }

    public <E> E subtrace(String id, Callable<E> task) {
//        if (!enabled) {
//            return;
//        }

        Trace parent = currentSubTrace;
        Trace current = new Trace(id);
        currentSubTrace = current;
        currentSubTrace.parentTrace = parent;
        parent.subTraces.add(currentSubTrace);
//        stack.push(currentSubTrace);
//
//        Trace trace = currentSubTrace;
//        trace.start(id);
        try {
            return task.call();
        } catch (Exception e) {
            failureMessage = e.getMessage();
            throw rethrow(e);
        } finally {
            //          Trace entry = current.stack.pop();
            current.endNs = nanoTime();
            currentSubTrace = current.parentTrace;
        }
    }

    public String toString() {
        if (!enabled) {
            return null;
        }

        StringBuffer sb = new StringBuffer();
        toString(1, sb);
        return sb.toString();
    }

    private long durationNs() {
        return endNs - startNs;
    }

    private void toString(int nesting, StringBuffer sb) {
        if (isSimple()) {
            long durationNs = durationNs();
            if (durationNs < MICROSECONDS.toNanos(MIN_LATENCY_US)) {
                return;
            }

            sb.append(indent(nesting - 1)).append(text).append(':');
            appendTime(sb, durationNs);
            sb.append("\n");
            return;
        }

        sb.append(indent(nesting - 1)).append(text).append("\n");

        String indent = indent(nesting);
        sb.append(indent).append("duration:");
        long totalNs = durationNs();
        appendTime(sb, totalNs);

        long accountedNs = 0;
        for (Trace subTrace : subTraces) {
            accountedNs += subTrace.durationNs();
        }

        sb.append(" accounted:");
        appendTime(sb, accountedNs);

        long unaccountedNs = totalNs - accountedNs;
        sb.append(" unaccounted:");
        appendTime(sb, unaccountedNs);
        sb.append("\n");

        if (failureMessage != null) {
            sb.append(indent).append("failureMessage:").append(failureMessage).append("\n");
        }

        if (meta != null) {
            for (Map.Entry<String, Object> entry : meta.entrySet()) {
                sb.append(indent).append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
            }
        }

        for (Trace subTrace : subTraces) {
            subTrace.toString(nesting + 1, sb);
        }
    }

    private boolean isSimple() {
        if (failureMessage != null) {
            return false;
        }

        if (subTraces == null || subTraces.isEmpty()) {
            return true;
        }

        return false;
    }

    private void appendTime(StringBuffer sb, long nanos) {
        if (nanos > MILLISECONDS.toNanos(MIN_DURATION)) {
            sb.append(NANOSECONDS.toMillis(nanos)).append(" ms");
        } else {
            sb.append(NANOSECONDS.toMicros(nanos)).append(" us");
        }
    }

    // todo: can be cached.
    private String indent(int nesting) {
        String indent = "";
        for (int k = 0; k < nesting; k++) {
            indent += "___";
        }
        return indent;
    }

//    public static class Entry {
//        final String id;
//        final long startNs = nanoTime();
//        long endNs;
//
//        private Entry(String id) {
//            this.id = id;
//        }
//
//        public void complete() {
//
//        }
//    }

    public static Trace getThreadLocalTrace() {
        return THREAD_LOCAL.get();
    }

    public static Trace setThreadLocalTrace(Trace trace) {
        THREAD_LOCAL.set(trace);
        return trace;
    }

    public static void removeThreadLocalTrace() {
        THREAD_LOCAL.remove();
    }
}
