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
import com.hazelcast.core.IExecutorService;
import com.hazelcast.test.annotation.QuickTest;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.pipeline.notification.Notification;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.cdc.SnapshotCompletionListener.NotificationStatus.DONE;
import static com.hazelcast.jet.cdc.SnapshotCompletionListener.NotificationStatus.FAILED;
import static com.hazelcast.jet.cdc.SnapshotCompletionListener.NotificationStatus.NOT_DONE;
import static com.hazelcast.jet.cdc.SnapshotCompletionListener.NotificationStatus.UNKNOWN;
import static com.hazelcast.jet.cdc.impl.NotificationHoldingMap.INSTANCE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
@QuickTest
class SnapshotCompletionListenerTest {
    @BeforeEach
    @AfterEach
    void clearStatuses() {
        INSTANCE.clearStatus(null, null);
    }

    @Test
    void twoVerticesCompleteOnlyAfterBothNotifications() {
        var first = listener("1234", "first");
        var second = listener("1234", "second");
        assertThat(INSTANCE.isCompletedFor(1234, null)).isFalse();
        send(first, "Initial Snapshot", "COMPLETED");
        assertThat(INSTANCE.isCompletedFor(1234, "first")).isTrue();
        assertThat(INSTANCE.isCompletedFor(1234, "second")).isFalse();
        assertThat(INSTANCE.isCompletedFor(1234, null)).isFalse();
        send(second, "Initial Snapshot", "COMPLETED");
        assertThat(INSTANCE.isCompletedFor(1234, null)).isTrue();
    }

    @Test
    void twoJobsHaveIndependentStatuses() {
        var first = listener("1234", "source");
        listener("5678", "source");
        send(first, "Initial Snapshot", "COMPLETED");
        assertThat(INSTANCE.isCompletedFor(1234, null)).isTrue();
        assertThat(INSTANCE.isCompletedFor(5678, null)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"COMPLETED", "SKIPPED", "completed", "skipped"})
    void completionNotifications(String type) {
        var listener = listener("1234", "source");
        send(listener, "Initial Snapshot", type);
        assertThat(INSTANCE.isCompletedFor(1234, null)).isTrue();
    }

    @Test
    void unrelatedNotificationsDoNotCompleteInitialSnapshot() {
        var listener = listener("1234", "source");
        send(listener, "Incremental Snapshot", "COMPLETED");
        send(listener, "Initial Snapshot", "STARTED");
        assertThat(INSTANCE.isCompletedFor(1234, null)).isFalse();
    }

    @Test
    void abortedSnapshotIsRecordedAsFailed() {
        var listener = listener("1234", "source");
        send(listener, "Initial Snapshot", "ABORTED");
        assertThat(INSTANCE.statusFor(1234, "source")).isEqualTo(FAILED);
        assertThat(INSTANCE.statusFor(1234, null)).isEqualTo(FAILED);
    }

    @Test
    void invalidJobIdIsRejected() {
        assertThatThrownBy(() -> listener("invalid", "source"))
                .isInstanceOf(HazelcastException.class).hasMessageContaining("Unable to parse job id");
    }

    @Test
    void missingVertexNameIsRejected() {
        assertThatThrownBy(() -> listener("1234", null))
                .isInstanceOf(HazelcastException.class).hasMessageContaining("No source vertex name");
    }

    @Test
    void completionRequiresAllParticipatingMembers() {
        var instance = mock(HazelcastInstance.class);
        var executor = mock(IExecutorService.class);
        when(instance.getExecutorService(SnapshotCompletionListener.EXECUTOR_NAME)).thenReturn(executor);
        var first = mock(Member.class);
        var second = mock(Member.class);
        var idle = mock(Member.class);
        doReturn(Map.of(first, CompletableFuture.completedFuture(DONE),
                        second, CompletableFuture.completedFuture(NOT_DONE),
                        idle, CompletableFuture.completedFuture(UNKNOWN)))
                .when(executor).submitToAllMembers(any(Callable.class));
        assertThat(SnapshotCompletionListener.isSnapshotCompleted(instance, 1234, null)).isFalse();
        doReturn(Map.of(first, CompletableFuture.completedFuture(DONE),
                        second, CompletableFuture.completedFuture(DONE),
                        idle, CompletableFuture.completedFuture(UNKNOWN)))
                .when(executor).submitToAllMembers(any(Callable.class));
        assertThat(SnapshotCompletionListener.isSnapshotCompleted(instance, 1234, null)).isTrue();
        doReturn(Map.of(idle, CompletableFuture.completedFuture(UNKNOWN)))
                .when(executor).submitToAllMembers(any(Callable.class));
        assertThat(SnapshotCompletionListener.isSnapshotCompleted(instance, 1234, null)).isFalse();
        doReturn(Map.of(first, CompletableFuture.completedFuture(DONE),
                        second, CompletableFuture.completedFuture(FAILED)))
                .when(executor).submitToAllMembers(any(Callable.class));
        assertThat(SnapshotCompletionListener.isSnapshotCompleted(instance, 1234, null)).isFalse();
    }

    @Test
    void distributedClearPropagatesMemberFailure() {
        var instance = mock(HazelcastInstance.class);
        var executor = mock(IExecutorService.class);
        when(instance.getExecutorService(SnapshotCompletionListener.EXECUTOR_NAME)).thenReturn(executor);
        doReturn(Map.of(mock(Member.class), CompletableFuture.failedFuture(new IllegalStateException("clear failed"))))
                .when(executor).submitToAllMembers(any(Callable.class));
        assertThatThrownBy(() -> SnapshotCompletionListener.clearStatuses(instance))
                .isInstanceOf(IllegalStateException.class).hasMessage("clear failed");
    }

    @SuppressWarnings("deprecation")
    private static SnapshotCompletionListener listener(String jobId, String vertexName) {
        var builder = Configuration.create().with("notification.SnapshotCompletionListener.jobId", jobId);
        if (vertexName != null) {
            builder.with("notification.SnapshotCompletionListener.sourceVertexName", vertexName);
        }
        var config = mock(CommonConnectorConfig.class);
        when(config.getConfig()).thenReturn(builder.build());
        var listener = new SnapshotCompletionListener();
        listener.init(config);
        return listener;
    }

    private static void send(SnapshotCompletionListener listener, String aggregate, String type) {
        var notification = mock(Notification.class);
        when(notification.getAggregateType()).thenReturn(aggregate);
        when(notification.getType()).thenReturn(type);
        listener.send(notification);
    }
}
