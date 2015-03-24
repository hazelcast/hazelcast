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

import com.hazelcast.internal.management.dto.SlowOperationDTO;
import com.hazelcast.internal.management.dto.SlowOperationInvocationDTO;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Internal data structure for {@link SlowOperationDetector}.
 * <p/>
 * A collection of this class is created by {@link SlowOperationDetector} and shared as <code>Collection<JsonSerializable></code>
 * with {@link com.hazelcast.monitor.impl.LocalOperationStatsImpl} to deliver a JSON representation for the Management Center.
 * <p/>
 * All fields are exclusively written by {@link SlowOperationDetector.DetectorThread}. Only fields which are exposed via the
 * {@link #createDTO()} methods need synchronization. All other fields are final or used single threaded.
 */
final class SlowOperationLog {

    private static final int SHORT_STACKTRACE_LENGTH = 200;

    final String operation;
    final String stackTrace;
    final String shortStackTrace;

    // this field will be incremented by single SlowOperationDetectorThread (it can be read by multiple threads)
    volatile int totalInvocations;

    private final ConcurrentHashMap<Integer, Invocation> invocations = new ConcurrentHashMap<Integer, Invocation>();

    SlowOperationLog(String stackTrace, Object operation) {
        this.operation = operation.toString();
        this.stackTrace = stackTrace;
        if (stackTrace.length() <= SHORT_STACKTRACE_LENGTH) {
            this.shortStackTrace = stackTrace;
        } else {
            this.shortStackTrace = stackTrace.substring(0, stackTrace.indexOf('\n', SHORT_STACKTRACE_LENGTH)) + "\n(...)";
        }
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
        for (Map.Entry<Integer, Invocation> invocationEntry : invocations.entrySet()) {
            if (nowNanos - invocationEntry.getValue().lastAccessNanos > slowOperationLogLifetimeNanos) {
                invocations.remove(invocationEntry.getKey());
            }
        }
        return invocations.isEmpty();
    }

    SlowOperationDTO createDTO() {
        List<SlowOperationInvocationDTO> invocationDTOList = new ArrayList<SlowOperationInvocationDTO>(invocations.size());
        for (Invocation invocation : invocations.values()) {
            invocationDTOList.add(invocation.createDTO());
        }
        return new SlowOperationDTO(operation, stackTrace, totalInvocations, invocationDTOList);
    }

    static final class Invocation {

        private final int id;
        private final long startedAt;

        // this field will be written by single SlowOperationDetectorThread (it can be read by multiple threads)
        private volatile int durationMs;

        private long lastAccessNanos;

        private Invocation(int id, long startedAt, long lastAccessNanos, int durationMs) {
            this.id = id;
            this.startedAt = startedAt;
            this.durationMs = durationMs;
            this.lastAccessNanos = lastAccessNanos;
        }

        void update(long lastAccessNanos, int durationMs) {
            this.durationMs = durationMs;
            this.lastAccessNanos = lastAccessNanos;
        }

        SlowOperationInvocationDTO createDTO() {
            return new SlowOperationInvocationDTO(id, startedAt, durationMs);
        }
    }
}
