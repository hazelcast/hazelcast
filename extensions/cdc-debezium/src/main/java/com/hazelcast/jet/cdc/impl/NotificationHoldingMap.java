/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.cdc.impl;

import com.hazelcast.jet.cdc.SnapshotCompletionListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.jet.cdc.SnapshotCompletionListener.NotificationStatus.DONE;
import static com.hazelcast.jet.cdc.SnapshotCompletionListener.NotificationStatus.FAILED;
import static com.hazelcast.jet.cdc.SnapshotCompletionListener.NotificationStatus.NOT_DONE;
import static com.hazelcast.jet.cdc.SnapshotCompletionListener.NotificationStatus.UNKNOWN;
import static java.util.Objects.requireNonNullElse;

public class NotificationHoldingMap {
    public static final NotificationHoldingMap INSTANCE = new NotificationHoldingMap();
    /**
     * Key when keeping the status is per-job, not per-vertex
     */
    private static final String GENERAL = "__jet.GENERAL_KEY";
    // Map<Job -> Map<Vertex -> STATUS>>
    private final Map<Long, Map<String, SnapshotCompletionListener.NotificationStatus>> completionStatuses =
        new ConcurrentHashMap<>();

    public void markNotYetDone(long jobId, String sourceVertexName) {
        var statusMap = completionStatuses.computeIfAbsent(jobId, k -> new ConcurrentHashMap<>());
        if (sourceVertexName != null) {
            statusMap.put(sourceVertexName, NOT_DONE);
        }
        statusMap.put(GENERAL, anyFailedExceptGeneral(statusMap) ? FAILED : NOT_DONE);
    }

    public void markDone(long jobId, String sourceVertexName) {
        var statusMap = completionStatuses.computeIfAbsent(jobId, k -> new ConcurrentHashMap<>());

        if (sourceVertexName != null) {
            statusMap.put(sourceVertexName, DONE);
        }
        if (allAreDoneExceptGeneral(statusMap) && statusMap.containsKey(GENERAL)) {
            statusMap.put(GENERAL, DONE);
        }
    }

    public void markFailed(long jobId, String sourceVertexName) {
        var statusMap = completionStatuses.computeIfAbsent(jobId, k -> new ConcurrentHashMap<>());
        statusMap.put(sourceVertexName, FAILED);
        statusMap.put(GENERAL, FAILED);
    }

    private boolean anyFailedExceptGeneral(Map<String, SnapshotCompletionListener.NotificationStatus> statusMap) {
        return statusMap.entrySet().stream().anyMatch(e -> e.getValue() == FAILED && !e.getKey().equals(GENERAL));
    }

    private boolean allAreDoneExceptGeneral(Map<String, SnapshotCompletionListener.NotificationStatus> statusMap) {
        return statusMap.entrySet().stream().allMatch(e -> e.getValue() == DONE || e.getKey().equals(GENERAL));
    }

    public boolean isCompletedFor(long jobId, String sourceVertexName) {
        return statusFor(jobId, sourceVertexName) == DONE;
    }

    public SnapshotCompletionListener.NotificationStatus statusFor(long jobId, String sourceVertexName) {
        var statusMap = completionStatuses.get(jobId);
        return statusMap == null ? UNKNOWN : statusMap.getOrDefault(requireNonNullElse(sourceVertexName, GENERAL), UNKNOWN);
    }

    public void clearStatus(Long jobId, String vertexName) {
        if (jobId == null) {
            completionStatuses.clear();
        } else if (vertexName == null) {
            completionStatuses.remove(jobId);
        } else {
            completionStatuses.computeIfPresent(jobId, (id, perVertex) -> {
                perVertex.remove(vertexName);
                if (perVertex.size() == 1 && perVertex.containsKey(GENERAL)) {
                    return null;
                }
                perVertex.put(GENERAL, anyFailedExceptGeneral(perVertex) ? FAILED
                        : allAreDoneExceptGeneral(perVertex) ? DONE : NOT_DONE);
                return perVertex;
            });
        }
    }

    public boolean isEmpty() {
        return completionStatuses.isEmpty();
    }
}
