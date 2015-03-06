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

package com.hazelcast.spi.impl.slowoperationdetector;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.management.JsonSerializable;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.JsonUtil.getArray;
import static com.hazelcast.util.JsonUtil.getInt;
import static com.hazelcast.util.JsonUtil.getLong;
import static com.hazelcast.util.JsonUtil.getString;

/**
 * Data structure for slow operation logs.
 * <p/>
 * A single instance of this class is created by {@link SlowOperationDetector} and shared with
 * {@link com.hazelcast.monitor.impl.LocalOperationStatsImpl} to deliver a JSON representation for the Management Center.
 * <p/>
 * Data is exclusively written by {@link SlowOperationDetector} and just read by other threads.
 */
public final class SlowOperationLog implements JsonSerializable {
    private static final int SHORT_STACKTRACE_LENGTH = 200;

    private static final ConstructorFunction<Integer, Invocation> INVOCATION_CONSTRUCTOR_FUNCTION
            = new ConstructorFunction<Integer, Invocation>() {
        @Override
        public Invocation createNew(Integer arg) {
            return new Invocation();
        }
    };

    private final ConcurrentHashMap<Integer, Invocation> invocations = new ConcurrentHashMap<Integer, Invocation>();

    // values are just set once, but cannot be final due to JsonSerializable
    private String operation;
    private String stackTrace;

    // this field will be incremented by SlowOperationDetectorThread only (it can be read by multiple threads)
    private volatile int totalInvocations;

    // only accessed by a single SlowOperationDetectorThread, no need for synchronization
    private transient String shortStackTrace;

    public SlowOperationLog(String stackTrace, Object operation) {
        this.operation = operation.toString();
        this.stackTrace = stackTrace;
        this.totalInvocations = 0;
    }

    String getOperation() {
        return operation;
    }

    String getStackTrace() {
        return stackTrace;
    }

    String getShortStackTrace() {
        if (shortStackTrace == null) {
            shortStackTrace = stackTrace.substring(0, stackTrace.indexOf('\n', SHORT_STACKTRACE_LENGTH)) + "\n(...)";
        }
        return shortStackTrace;
    }

    int getTotalInvocations() {
        return totalInvocations;
    }

    Collection<Invocation> getInvocations() {
        return invocations.values();
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "VO_VOLATILE_INCREMENT", justification =
            "method can be accessed by a single SlowOperationDetectorThread only")
    Invocation getOrCreateInvocation(int operationHashCode, long lastDurationNanos, long nowNanos, long nowMillis) {
        totalInvocations++;

        Invocation invocation = getOrPutIfAbsent(invocations, operationHashCode, INVOCATION_CONSTRUCTOR_FUNCTION);
        invocation.id = operationHashCode;
        invocation.startedAt = nowMillis - invocation.durationNanos;
        invocation.durationNanos = TimeUnit.NANOSECONDS.toMillis(lastDurationNanos);
        invocation.lastAccessNanos = nowNanos;

        return invocation;
    }

    void purgeInvocations(long nowNanos, long slowOperationLogLifetimeNanos) {
        for (Map.Entry<Integer, Invocation> invocationEntry : invocations.entrySet()) {
            if (nowNanos - invocationEntry.getValue().lastAccessNanos > slowOperationLogLifetimeNanos) {
                invocations.remove(invocationEntry.getKey());
            }
        }
    }

    boolean isEmpty() {
        return invocations.isEmpty();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("SlowOperationLog{operation='").append(operation)
                .append("', stackTrace='").append(stackTrace)
                .append("', totalInvocations=").append(totalInvocations)
                .append(", invocations='");

        String prefix = "";
        for (Invocation log : invocations.values()) {
            sb.append(prefix).append(log.toString());
            prefix = ", ";
        }

        sb.append("'}");
        return sb.toString();
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("operation", operation);
        root.add("stackTrace", stackTrace);
        root.add("totalInvocations", totalInvocations);
        JsonArray invocationArray = new JsonArray();
        for (Map.Entry<Integer, Invocation> invocation : invocations.entrySet()) {
            invocation.getValue().id = invocation.getKey();
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
            Invocation invocation = new Invocation();
            invocation.fromJson(jsonValue.asObject());
            invocations.put(invocation.id, invocation);
        }
    }

    static final class Invocation implements JsonSerializable {
        int id;
        long startedAt;
        long durationNanos;
        volatile long lastAccessNanos;

        public long getDurationNanos() {
            return durationNanos;
        }

        @Override
        public String toString() {
            // NOP to read lastAccess to update local thread values
            if (lastAccessNanos < 0) {
                assert true;
            }

            return "Invocation{" + "id=" + id + ", startedAt=" + startedAt + ", durationNanos=" + durationNanos + '}';
        }

        @Override
        public JsonObject toJson() {
            // NOP to read lastAccess to update local thread values
            if (lastAccessNanos < 0) {
                assert true;
            }

            JsonObject root = new JsonObject();
            root.add("id", id);
            root.add("startedAt", startedAt);
            root.add("duration", durationNanos);
            return root;
        }

        @Override
        public void fromJson(JsonObject json) {
            id = getInt(json, "id");
            startedAt = getLong(json, "startedAt");
            durationNanos = getLong(json, "duration");
            lastAccessNanos = System.nanoTime();
        }
    }
}
