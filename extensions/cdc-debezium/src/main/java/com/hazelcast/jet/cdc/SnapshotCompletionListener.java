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
package com.hazelcast.jet.cdc;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.ThrowingFunction;
import com.hazelcast.jet.Job;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.notification.channels.NotificationChannel;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static java.util.stream.Collectors.toMap;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.cdc.impl.NotificationHoldingMap.INSTANCE;

/**
 * Tracks Debezium initial snapshots for sources with snapshot tracking enabled.
 * A restored source reports completion when Debezium emits {@code SKIPPED};
 * a source configured with {@code snapshot.mode=always} must finish its new snapshot.
 * <p>
 * To query one source, assign a unique name using
 * {@link com.hazelcast.jet.pipeline.StreamStage#setName(String)} on the source stage
 * and pass that name as {@code sourceVertexName}. Without unique names, the pipeline
 * may rename duplicate vertices when the job is submitted.
 * <p>
 * Listener uses {@code SnapshotCompletionListener} executor to check and clear completion statuses on all members.
 * If you need to configure security or want to customize other parameters of executor that will handle these calls,
 * please update configuration for {@code SnapshotCompletionListener} executor.
 *
 * @since 6.0
 */
public class SnapshotCompletionListener implements NotificationChannel {

    /**
     * Status of an initial snapshot.
     *
     * @since 6.0
     */
    public enum NotificationStatus {

        /**
         * Snapshot is done for some job and vertex on some node.
         */
        DONE,

        /**
         * Snapshot is registered to be expected, but is not yet done for some job and vertex on some node.
         */
        NOT_DONE,

        /**
         * The initial snapshot was aborted.
         */
        FAILED,

        /**
         * Snapshot was not registered to be expected and therefore is unknown for the node.
         */
        UNKNOWN

    }

    static final String EXECUTOR_NAME = "SnapshotCompletionListener";

    private static final ILogger LOGGER = Logger.getLogger(SnapshotCompletionListener.class);

    private long jobId;
    private String sourceVertexName;

    public SnapshotCompletionListener() {
    }

    @Override
    @SuppressWarnings("deprecation")
    public void init(CommonConnectorConfig config) {
        try {
            jobId = Long.parseLong(config.getConfig().getString("notification.SnapshotCompletionListener.jobId"));
        } catch (NumberFormatException e) {
            throw new HazelcastException("Unable to parse job id from job config. "
                                             + "Did you forget to call .enableSnapshotTracking() on source builder?", e);
        }
        sourceVertexName = config.getConfig().getString("notification.SnapshotCompletionListener.sourceVertexName");
        if (sourceVertexName == null) {
            throw new HazelcastException("No source vertex name configured for SnapshotCompletionListener. "
                                             + "Did you forget to call .enableSnapshotTracking() on source builder?");
        }
        INSTANCE.markNotYetDone(jobId, sourceVertexName);
    }

    @Override
    public String name() {
        return "SnapshotCompletionListener";
    }

    @Override
    public void send(Notification notification) {
        if (LOGGER.isFinestEnabled()) {
            LOGGER.finest("Got notification of type " + notification.getAggregateType() + ": " + notification);
        }
        if (notification.getAggregateType().equalsIgnoreCase("Initial Snapshot")) {
            if ("COMPLETED".equalsIgnoreCase(notification.getType())) {
                LOGGER.fine("Snapshot completed for job with id " + jobId);
                markCompleted();
            } else if ("ABORTED".equalsIgnoreCase(notification.getType())) {
                LOGGER.warning("Snapshot aborted for job with id " + jobId);
                INSTANCE.markFailed(jobId, sourceVertexName);
            } else if ("SKIPPED".equalsIgnoreCase(notification.getType())) {
                LOGGER.fine("Snapshot skipped for job with id " + jobId);
                markCompleted();
            }
        }
    }

    void markCompleted() {
        INSTANCE.markDone(jobId, sourceVertexName);
    }

    @Override
    public void close() {
    }

    /**
     * Checks whether every registered tracked source of a running job has completed its initial snapshot.
     *
     * @param instance member or client connected to the job's cluster
     * @param job job to query
     * @return true if at least one tracked source is known and all known sources are done;
     *         false if an initial snapshot was aborted
     * @since 6.0
     */
    public static boolean isSnapshotCompleted(@Nonnull HazelcastInstance instance, @Nonnull Job job) {
        return isSnapshotCompleted(instance, job.getId(), null);
    }

    /**
     * Checks initial snapshot completion on every member.
     *
     * @param instance member or client connected to the job's cluster
     * @param id job ID
     * @param sourceVertexName unique source stage name, or null to query all tracked sources of the job
     * @return true if at least one matching source is known and all matching sources are done;
     *         false if an initial snapshot was aborted
     * @since 6.0
     */
    public static boolean isSnapshotCompleted(@Nonnull HazelcastInstance instance, long id, @Nullable String sourceVertexName) {
        Map<Member, NotificationStatus> submitted = instance
            .getExecutorService(EXECUTOR_NAME)
            .submitToAllMembers(new AskForSnapshotTask(id, sourceVertexName))
            .entrySet().stream()
            .map(ThrowingFunction.wrap(e -> Map.entry(e.getKey(), e.getValue().get())))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (LOGGER.isFineEnabled()) {
            LOGGER.fine("Completion results for %s: %s".formatted(id, submitted));
        }
        List<NotificationStatus> values = submitted.values().stream().distinct().toList();
        if (values.contains(NotificationStatus.FAILED)) {
            LOGGER.severe("Initial snapshot aborted for job " + id
                                                 + (sourceVertexName == null ? "" : ", source " + sourceVertexName));
            return false;
        }
        return values.stream().allMatch(b -> b == NotificationStatus.DONE || b == NotificationStatus.UNKNOWN)
            &&  values.stream().anyMatch(b -> b == NotificationStatus.DONE);
    }

    /**
     * Clears all initial snapshot statuses on all members, waiting for completion.
     * Call only when tracked jobs have stopped; clearing active sources loses their registration.
     *
     * @param instance member or client connected to the cluster
     * @since 6.0
     */
    public static void clearStatuses(@Nonnull HazelcastInstance instance) {
        clearStatus(instance, null, null);
    }

    /**
     * Clears matching initial snapshot statuses on all members, waiting for completion.
     * Call only after the affected sources have stopped.
     *
     * @param instance member or client connected to the cluster
     * @param jobId job to clear, or null to clear all jobs
     * @param vertexName source vertex to clear, or null to clear all sources of the job
     * @since 6.0
     */
    public static void clearStatus(@Nonnull HazelcastInstance instance, @Nullable Long jobId, @Nullable String vertexName) {
        var results = instance.getExecutorService(EXECUTOR_NAME)
                              .submitToAllMembers(new ClearStatusTask(jobId, vertexName));
        for (var result : results.values()) {
            try {
                result.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw rethrow(e);
            } catch (ExecutionException e) {
                throw rethrow(e.getCause());
            }
        }
    }

    private record AskForSnapshotTask(long jobId, String vertexName) implements Callable<NotificationStatus>, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        @Override
        public NotificationStatus call() {
            return INSTANCE.statusFor(jobId, vertexName);
        }
    }

    private static class ClearStatusTask implements Callable<Boolean>, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private Long jobId;
        private String vertexName;

        @SuppressWarnings("unused")
        ClearStatusTask() {
        }

        private ClearStatusTask(Long jobId, String vertexName) {
            this.jobId = jobId;
            this.vertexName = vertexName;
        }

        @Override
        public Boolean call() {
            INSTANCE.clearStatus(jobId, vertexName);
            return true;
        }
    }
}
