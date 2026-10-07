/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.snapshot;

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.snapshot.DataChunkGroup;
import com.hazelcast.cp.internal.raft.ChunkedSnapshotAwareService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.properties.HazelcastProperties;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.hazelcast.internal.serialization.impl.HeapData.DATA_OFFSET;
import static com.hazelcast.internal.serialization.impl.HeapData.HEAP_DATA_OVERHEAD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test base class that verifies the immutability guarantees of
 * chunked snapshots produced by CP subsystem services.
 *
 * <p>In Raft, once a snapshot (or chunk group of a snapshot) is taken,
 * the snapshot data becomes part of the Raft log and MUST behave as an
 * immutable value object. Neither:
 *
 * <ul>
 *   <li>mutations performed on the service after the snapshot was taken, nor</li>
 *   <li>invocations of {@code restoreChunkedSnapshot()} on another service</li>
 * </ul>
 *
 * may alter the snapshot objects that were produced earlier.
 *
 * <p>This test enforces the following contract:
 *
 * <ol>
 *   <li>Create a service instance and populate it with non-trivial state.</li>
 *   <li>Take a chunked snapshot and record a fingerprint of each chunk group.</li>
 *   <li>Mutate the service after the snapshot: snapshot groups must remain unchanged.</li>
 *   <li>Restore the snapshot into a fresh service: restore must not mutate the snapshot groups.</li>
 *   <li>Mutate the restored service: stored snapshot groups must still remain unchanged.</li>
 * </ol>
 *
 * <p>Any violation indicates that the implementation is leaking references
 * or mutating snapshot state, which would break Raft snapshot correctness
 * (followers could diverge after receiving mutated snapshots).
 *
 * @param <Chunk>    the type of chunk elements contained in each chunk group
 * @param <Snapshot> the logical snapshot type produced by the service
 * @param <Service>  the CP service under test, which must support chunked snapshotting
 */
public abstract class ChunkedSnapshotImmutabilityTest<Chunk, Snapshot, Service
            extends ChunkedSnapshotAwareService<Chunk, Snapshot>> {

        protected abstract Service newService();

        protected abstract CPGroupId groupId();

        /** Populate non-trivial state in the service. */
        protected abstract void populateInitialState(Service service, CPGroupId groupId);

        /** Mutate the service in a typical way. */
        protected abstract void mutateService(Service service, CPGroupId groupId);

        /**
         * Produce a fingerprint for a chunk group.
         */
        protected abstract Object chunkGroupFingerprint(DataChunkGroup<Chunk> group);

        @Test
        public void chunkGroupsAreNotAffectedByLaterMutationsOrRestore() {
            CPGroupId gid = groupId();

            // Original service with some state
            Service service = newService();
            populateInitialState(service, gid);

            // Take chunked snapshot and copy groups (simulate Raft log storage)
            long commitIndex = 1L;
            List<DataChunkGroup<Chunk>> groups = new ArrayList<>();
            service.takeSnapshotChunks(gid, commitIndex).forEachRemaining(groups::add);

            List<Object> fingerprintsBefore =
                    groups.stream().map(this::chunkGroupFingerprint).collect(Collectors.toList());

            // Mutate original service AFTER snapshot
            mutateService(service, gid);

            // Snapshot groups must still look the same
            List<Object> fingerprintsAfterMutation =
                    groups.stream().map(this::chunkGroupFingerprint).collect(Collectors.toList());
            assertThat(fingerprintsAfterMutation)
                    .as("Chunk groups must not be affected by mutations after snapshot")
                    .isEqualTo(fingerprintsBefore);

            // Restore into a fresh service using these groups
            Service restored = newService();
            restored.prepareForSnapshotRestore(gid);
            for (DataChunkGroup<Chunk> group : groups) {
                restored.restoreSnapshotChunk(gid, commitIndex, group);
            }

            // Restoring must not mutate the stored groups
            List<Object> fingerprintsAfterRestore =
                    groups.stream().map(this::chunkGroupFingerprint).collect(Collectors.toList());
            assertThat(fingerprintsAfterRestore)
                    .as("restoring chunked snapshot must not mutate the stored chunk groups")
                    .isEqualTo(fingerprintsBefore);

            // Mutate restored service
            mutateService(restored, gid);

            // Groups must remain be unchanged
            List<Object> fingerprintsAfterSecondMutation =
                    groups.stream().map(this::chunkGroupFingerprint).collect(Collectors.toList());
            assertThat(fingerprintsAfterSecondMutation)
                    .as("Mutating service after restore must not change the stored chunk groups")
                    .isEqualTo(fingerprintsBefore);
        }

     NodeEngine mockNodeEngineForChunking() {
        NodeEngine engine = mock(NodeEngine.class);

        Config config = new Config();
        config.setCPSubsystemConfig(new CPSubsystemConfig());
        when(engine.getConfig()).thenReturn(config);

        HazelcastProperties hzProps = new HazelcastProperties(new Properties());
        when(engine.getProperties()).thenReturn(hzProps);

        RaftService raftService = mock(RaftService.class);
        when(raftService.isCpSubsystemEnabled()).thenReturn(true);
        when(engine.getService(RaftService.SERVICE_NAME)).thenReturn(raftService);

        return engine;
    }

     static Data data(String value) {
        byte[] strBytes = value.getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[HEAP_DATA_OVERHEAD + strBytes.length];
        System.arraycopy(strBytes, 0, payload, DATA_OFFSET, strBytes.length);
        return new HeapData(payload);
    }
}
