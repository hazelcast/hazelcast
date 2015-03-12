/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationexecutor.slowoperationdetector;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.management.JsonSerializable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.JsonUtil.getArray;
import static com.hazelcast.util.JsonUtil.getInt;
import static com.hazelcast.util.JsonUtil.getLong;
import static com.hazelcast.util.JsonUtil.getString;

/**
 * Internal data structure for {@link SlowOperationDetector}.
 * <p/>
 * A collection of this class is created by {@link SlowOperationDetector} and shared as <code>Collection<JsonSerializable></code>
 * with {@link com.hazelcast.monitor.impl.LocalOperationStatsImpl} to deliver a JSON representation for the Management Center.
 * <p/>
 * All data is exclusively written by {@link SlowOperationDetector.DetectorThread} and maybe read by other threads. Only fields
 * which are exposed via the {@link #toJson()} method need synchronization. All other fields are used single threaded.
 */
final class SlowOperationLog implements JsonSerializable {

    private static final int SHORT_STACKTRACE_LENGTH = 200;

    private final ConcurrentHashMap<Integer, Invocation> invocations = new ConcurrentHashMap<Integer, Invocation>();

    // values are just set once, but cannot be final due to JsonSerializable
    private String operation;
    private String stackTrace;

    // this field will be incremented by single SlowOperationDetectorThread (it can be read by multiple threads)
    private volatile int totalInvocations;

    // only accessed by single SlowOperationDetectorThread, no need for synchronization
    private transient String shortStackTrace;

    SlowOperationLog(String stackTrace, Object operation) {
        this.operation = operation.toString();
        this.stackTrace = stackTrace;
        this.totalInvocations = 0;
        if (stackTrace.length() <= SHORT_STACKTRACE_LENGTH) {
            this.shortStackTrace = stackTrace;
        } else {
            this.shortStackTrace = stackTrace.substring(0, stackTrace.indexOf('\n', SHORT_STACKTRACE_LENGTH)) + "\n(...)";
        }
    }

    SlowOperationLog(JsonObject json) {
        fromJson(json);
    }

    String getStackTrace() {
        // method is used by a single SlowOperationDetectorThread only
        return stackTrace;
    }

    String getShortStackTrace() {
        // method is used by a single SlowOperationDetectorThread only
        return shortStackTrace;
    }

    int getTotalInvocations() {
        // method is used by a single SlowOperationDetectorThread only
        return totalInvocations;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "VO_VOLATILE_INCREMENT", justification =
            "method is used by a single SlowOperationDetectorThread only")
    Invocation getOrCreateInvocation(Integer operationHashCode, long lastDurationNanos, long nowNanos, long nowMillis) {
        totalInvocations++;

        Invocation candidate = invocations.get(operationHashCode);
        if (candidate != null) {
            return candidate;
        }

        int durationMs = (int) TimeUnit.NANOSECONDS.toMillis(lastDurationNanos);
        long startedAt = nowMillis - durationMs;
        candidate = new Invocation(operationHashCode, startedAt, nowNanos, durationMs);
        invocations.put(operationHashCode, candidate);

        return candidate;
    }

    boolean purgeInvocations(long nowNanos, long slowOperationLogLifetimeNanos) {
        // method is used by a single SlowOperationDetectorThread only
        for (Map.Entry<Integer, Invocation> invocationEntry : invocations.entrySet()) {
            if (nowNanos - invocationEntry.getValue().lastAccessNanos > slowOperationLogLifetimeNanos) {
                invocations.remove(invocationEntry.getKey());
            }
        }
        return invocations.isEmpty();
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("operation", operation);
        root.add("stackTrace", stackTrace);
        root.add("totalInvocations", totalInvocations);
        JsonArray invocationArray = new JsonArray();
        for (Map.Entry<Integer, Invocation> invocation : invocations.entrySet()) {
            JsonObject json = invocation.getValue().toJson();
            if (json != null) {
                invocationArray.add(json);
            }
        }
        root.add("invocations", invocationArray);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        operation = getString(json, "operation");
        stackTrace = getString(json, "stackTrace");
        totalInvocations = getInt(json, "totalInvocations");

        JsonArray invocationArray = getArray(json, "invocations");
        for (JsonValue jsonValue : invocationArray) {
            Invocation invocation = new Invocation(jsonValue.asObject());
            invocations.put(invocation.id, invocation);
        }
    }

    static final class Invocation {

        private final int id;
        private final long startedAt;

        // field is written by a single SlowOperationDetectorThread only (it can be read by multiple threads)
        private volatile int durationMs;

        // field is used by a single SlowOperationDetectorThread only
        private transient long lastAccessNanos;

        private Invocation(int id, long startedAt, long lastAccessNanos, int durationMs) {
            this.id = id;
            this.startedAt = startedAt;
            this.durationMs = durationMs;
            this.lastAccessNanos = lastAccessNanos;
        }

        private Invocation(JsonObject json) {
            id = getInt(json, "id");
            startedAt = getLong(json, "startedAt");
            durationMs = getInt(json, "durationMs");
            lastAccessNanos = System.nanoTime();
        }

        void update(long lastAccessNanos, int durationMs) {
            // method is used by a single SlowOperationDetectorThread only
            this.durationMs = durationMs;
            this.lastAccessNanos = lastAccessNanos;
        }

        private JsonObject toJson() {
            JsonObject root = new JsonObject();
            root.add("id", id);
            root.add("startedAt", startedAt);
            root.add("durationMs", durationMs);
            return root;
        }
    }
}
