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

import io.debezium.snapshot.mode.InitialSnapshotter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test-only snapshotter that lets the two-source test release each initial snapshot independently.
 */
public class BlockingInitialSnapshotter extends InitialSnapshotter {
    private static final Map<String, Gate> GATES = new ConcurrentHashMap<>();
    private Gate gate;

    @Override
    public String name() {
        return "test-blocking-initial";
    }

    @Override
    public void configure(Map<String, ?> props) {
        super.configure(props);
        String vertexName = (String) props.get("notification.SnapshotCompletionListener.sourceVertexName");
        gate = GATES.get(vertexName);
        if (gate == null) {
            throw new IllegalStateException("No test gate registered for " + vertexName);
        }
    }

    @Override
    public boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress) {
        gate.started.countDown();
        await(gate.released);
        return super.shouldSnapshotData(offsetExists, snapshotInProgress);
    }

    static Gate block(String vertexName) {
        Gate gate = new Gate(vertexName);
        if (GATES.putIfAbsent(vertexName, gate) != null) {
            throw new IllegalStateException("Test gate already registered for " + vertexName);
        }
        return gate;
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(2, MINUTES)).as("snapshot test gate").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for snapshot test gate", e);
        }
    }

    static final class Gate implements AutoCloseable {
        private final String vertexName;
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        private Gate(String vertexName) {
            this.vertexName = vertexName;
        }

        void awaitStarted() {
            await(started);
        }

        void release() {
            released.countDown();
        }

        @Override
        public void close() {
            release();
            GATES.remove(vertexName, this);
        }
    }
}
