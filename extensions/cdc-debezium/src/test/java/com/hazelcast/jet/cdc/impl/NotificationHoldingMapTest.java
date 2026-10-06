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

import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;

import static com.hazelcast.jet.cdc.SnapshotCompletionListener.NotificationStatus.DONE;
import static com.hazelcast.jet.cdc.SnapshotCompletionListener.NotificationStatus.FAILED;
import static com.hazelcast.jet.cdc.SnapshotCompletionListener.NotificationStatus.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;

@QuickTest
class NotificationHoldingMapTest {

    @Test
    void readingUnknownJobsDoesNotRetainThem() {
        var statuses = new NotificationHoldingMap();
        assertThat(statuses.statusFor(1, null)).isEqualTo(UNKNOWN);
        assertThat(statuses.isCompletedFor(1, null)).isFalse();
        assertThat(statuses.isCompletedFor(1, "source")).isFalse();
        assertThat(statuses.isEmpty()).isTrue();
    }

    @Test
    void clearingOneVertexPreservesOtherVerticesAndRecomputesJobStatus() {
        var statuses = new NotificationHoldingMap();
        statuses.markNotYetDone(1, "first");
        statuses.markNotYetDone(1, "second");
        statuses.markDone(1, "first");
        assertThat(statuses.isCompletedFor(1, null)).isFalse();
        statuses.clearStatus(1L, "second");
        assertThat(statuses.statusFor(1, null)).isEqualTo(DONE);
        assertThat(statuses.statusFor(1, "first")).isEqualTo(DONE);
        statuses.clearStatus(1L, "first");
        assertThat(statuses.isEmpty()).isTrue();
    }

    @Test
    void clearingJobRemovesAllItsVerticesOnly() {
        var statuses = new NotificationHoldingMap();
        statuses.markNotYetDone(1, "first");
        statuses.markNotYetDone(1, "second");
        statuses.markNotYetDone(2, "first");
        statuses.clearStatus(1L, null);
        assertThat(statuses.statusFor(1, "first")).isEqualTo(UNKNOWN);
        assertThat(statuses.statusFor(1, "second")).isEqualTo(UNKNOWN);
        assertThat(statuses.isEmpty()).isFalse();
        statuses.clearStatus(2L, null);
        assertThat(statuses.isEmpty()).isTrue();
    }

    @Test
    void otherSourcesCannotHideAnAbortedSnapshot() {
        var statuses = new NotificationHoldingMap();
        statuses.markNotYetDone(1, "failed");
        statuses.markFailed(1, "failed");
        statuses.markNotYetDone(1, "other");
        assertThat(statuses.statusFor(1, null)).isEqualTo(FAILED);
        statuses.markDone(1, "other");
        assertThat(statuses.statusFor(1, null)).isEqualTo(FAILED);
        statuses.clearStatus(1L, "other");
        assertThat(statuses.statusFor(1, null)).isEqualTo(FAILED);
        statuses.clearStatus(1L, "failed");
        assertThat(statuses.statusFor(1, null)).isEqualTo(UNKNOWN);
    }
}
