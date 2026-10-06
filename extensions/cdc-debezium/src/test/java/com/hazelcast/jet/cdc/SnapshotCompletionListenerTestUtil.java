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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;

import java.time.Duration;

import static com.hazelcast.jet.cdc.SnapshotCompletionListener.isSnapshotCompleted;
import static org.awaitility.Awaitility.await;

/**
 * Utility to wait for CDC initial snapshot completion.
 *
 * @see SnapshotCompletionListener
 */
public final class SnapshotCompletionListenerTestUtil {

    private SnapshotCompletionListenerTestUtil() {
    }

    /**
     * Checks on all members if snapshot is completed for given Job.
     */
    public static void waitUntilSnapshotCompleted(HazelcastInstance instance, Job job) {
        await()
            .atMost(Duration.ofMinutes(5))
            .pollInterval(Duration.ofSeconds(10))
            .until(() -> isSnapshotCompleted(instance, job));
    }
    /**
     * Checks on all members if snapshot is completed for given Job.
     */
    public static void waitUntilSnapshotCompleted(HazelcastInstance instance, Job job, String vertexName) {
        await()
            .atMost(Duration.ofMinutes(5))
            .pollInterval(Duration.ofSeconds(10))
            .until(() -> isSnapshotCompleted(instance, job.getId() , vertexName));
    }
}
