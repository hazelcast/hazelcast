/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.diagnostics.OperationDescriptors;
import com.hazelcast.internal.management.dto.SlowOperationDTO;
import com.hazelcast.internal.management.dto.SlowOperationInvocationDTO;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Internal data structure for {@link SlowOperationDetector}.
 * <p>
 * A collection of this class is created by {@link SlowOperationDetector} and shared as <code>Collection<JsonSerializable></code>
 * with {@link com.hazelcast.internal.monitor.impl.LocalOperationStatsImpl}
 * to deliver a JSON representation for the Management Center.
 * <p>
 * All fields are exclusively written by {@link SlowOperationDetector.DetectorThread}. Only fields which are exposed via the
 * {@link #createDTO()} methods need synchronization. All other fields are final or used single threaded.
 */
final class SlowOperationLog {

    private static final int SHORT_STACKTRACE_LENGTH = 200;

    final AtomicInteger totalInvocations = new AtomicInteger(0);

    final String operation;
    final String stackTrace;
    final String shortStackTrace;

    private final ConcurrentHashMap<Integer, Invocation> invocations = new ConcurrentHashMap<Integer, Invocation>();

    SlowOperationLog(String stackTrace, Object task) {
        if (task instanceof Operation) {
            this.operation = OperationDescriptors.toOperationDesc((Operation) task);
        } else {
            this.operation = task.getClass().getName();
        }
        this.stackTrace = stackTrace;
        if (stackTrace.length() <= SHORT_STACKTRACE_LENGTH) {
            this.shortStackTrace = stackTrace;
        } else {
            this.shortStackTrace = stackTrace.substring(0, stackTrace.indexOf('\n', SHORT_STACKTRACE_LENGTH)) + "\n\t(...)";
        }
    }

    Invocation getOrCreate(Integer operationHashCode, Object operation, long lastDurationNanos, long nowNanos, long nowMillis) {
        Invocation candidate = invocations.get(operationHashCode);
        if (candidate != null) {
            return candidate;
        }

        int durationMs = (int) TimeUnit.NANOSECONDS.toMillis(lastDurationNanos);
        long startedAt = nowMillis - durationMs;
        candidate = new Invocation(operation.toString(), startedAt, nowNanos, durationMs);
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
        List<SlowOperationInvocationDTO> invocationDTOList = new ArrayList<>(invocations.size());
        for (Map.Entry<Integer, Invocation> invocationEntry : invocations.entrySet()) {
            int id = invocationEntry.getKey();
            invocationDTOList.add(invocationEntry.getValue().createDTO(id));
        }
        return new SlowOperationDTO(operation, stackTrace, totalInvocations.get(), invocationDTOList);
    }

    static final class Invocation {

        private final String operationDetails;
        private final long startedAt;

        private long lastAccessNanos;

        // this field will be written by single SlowOperationDetectorThread (it can be read by multiple threads)
        private volatile int durationMs;

        private Invocation(String operationDetails, long startedAt, long lastAccessNanos, int durationMs) {
            this.operationDetails = operationDetails;
            this.startedAt = startedAt;
            this.lastAccessNanos = lastAccessNanos;
            this.durationMs = durationMs;
        }

        void update(long lastAccessNanos, int durationMs) {
            this.lastAccessNanos = lastAccessNanos;
            this.durationMs = durationMs;
        }

        SlowOperationInvocationDTO createDTO(int id) {
            return new SlowOperationInvocationDTO(id, operationDetails, startedAt, durationMs);
        }
    }
}
