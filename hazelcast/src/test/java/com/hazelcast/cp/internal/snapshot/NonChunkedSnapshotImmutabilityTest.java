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
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import org.junit.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base test class that verifies the immutability guarantees of
 * non–chunked snapshots produced by CP subsystem services.
 *
 * <p>In the Raft algorithm, a snapshot returned by
 * {@link SnapshotAwareService#takeSnapshot(CPGroupId, long)} becomes part
 * of the replicated log. Once created, that snapshot MUST behave as an
 * immutable value object. Neither:
 *
 * <ul>
 *   <li>mutations performed on the service after the snapshot is taken, nor</li>
 *   <li>invocations of {@code restoreSnapshot()} on another service instance</li>
 * </ul>
 *
 * may modify the snapshot object that was previously returned.
 *
 * <p>This test enforces the following contract:
 *
 * <ol>
 *   <li>Create a service with non-trivial internal state.</li>
 *   <li>Invoke {@code takeSnapshot()} and compute a stable fingerprint of
 *       the snapshot (simulating what Raft log would store).</li>
 *   <li>Mutate the service after snapshot creation: the snapshot
 *       fingerprint must remain unchanged.</li>
 *   <li>Restore the snapshot into a new service instance: restore
 *       must NOT mutate the snapshot.</li>
 *   <li>Mutate the restored service: the original snapshot must still
 *       remain unchanged.</li>
 * </ol>
 *
 * <p>Any violation indicates leaked references or accidental mutation of
 * snapshot state, which would break Raft correctness guarantees and lead
 * to follower divergence.
 *
 * @param <Snapshot> the snapshot type produced by the service
 * @param <Service>  a CP service supporting non-chunked snapshotting
 */
public abstract class NonChunkedSnapshotImmutabilityTest<Snapshot, Service extends SnapshotAwareService<Snapshot>> {

    protected abstract Service newService();

    protected abstract CPGroupId groupId();

    /**
     * populate non-trivial state
     */
    protected abstract void populateInitialState(Service service, CPGroupId groupId);

    /**
     * typical mutations after snapshot
     */
    protected abstract void mutateService(Service service, CPGroupId groupId);

    /**
     * stable fingerprint for comparing snapshots
     */
    protected abstract Object snapshotFingerprint(Snapshot snapshot);

    @Test
    public void snapshotIsNotAffectedByLaterMutationsOrRestore() {
        CPGroupId gid = groupId();

        // Leader service with some state
        Service leader = newService();
        populateInitialState(leader, gid);

        // Take snapshot and compute fingerprint (what Raft stores in its log)
        long commitIndex = 1L;
        Snapshot snap = leader.takeSnapshot(gid, commitIndex);
        Object fingerprintBefore = snapshotFingerprint(snap);

        // Mutate leader AFTER snapshot
        mutateService(leader, gid);

        // Snapshot object must still look the same
        Object fingerprintAfterMutation = snapshotFingerprint(snap);
        assertThat(fingerprintAfterMutation)
                .as("Snapshot object must not be affected by later mutations")
                .isEqualTo(fingerprintBefore);

        // Restore into new follower service
        Service follower = newService();
        follower.restoreSnapshot(gid, commitIndex, snap);

        // Restore must not mutate snapshot
        Object fingerprintAfterRestore = snapshotFingerprint(snap);
        assertThat(fingerprintAfterRestore)
                .as("restoreSnapshot() must not mutate snapshot object")
                .isEqualTo(fingerprintBefore);

        // Follower becomes leader and mutates
        mutateService(follower, gid);

        // Snapshot must remain unchanged
        Object fingerprintAfterSecondMutation = snapshotFingerprint(snap);
        assertThat(fingerprintAfterSecondMutation)
                .as("Mutations on restored service must not change snapshot object")
                .isEqualTo(fingerprintBefore);
    }

    public NodeEngineImpl mockNodeEngine() {
        NodeEngineImpl engine = mock(NodeEngineImpl.class);

        // CP subsystem enabled (for AbstractCPMigrationAwareService)
        Config cfg = new Config();
        CPSubsystemConfig cpCfg = new CPSubsystemConfig();
        cpCfg.setCPMemberCount(3);
        cfg.setCPSubsystemConfig(cpCfg);

        doReturn(cfg).when(engine).getConfig();

        // Properties
        HazelcastProperties props = new HazelcastProperties(new Properties());
        when(engine.getProperties()).thenReturn(props);

        // Metrics
        MetricsRegistry metrics = mock(MetricsRegistry.class);
        when(engine.getMetricsRegistry()).thenReturn(metrics);

        // RaftService
        RaftService raftService = mock(RaftService.class);
        when(raftService.isCpSubsystemEnabled()).thenReturn(true);
        when(engine.getService(RaftService.SERVICE_NAME)).thenReturn(raftService);
        doReturn(cpCfg).when(raftService).getConfig();

        // ExecutionService + executor (used by RaftGroupMembershipManager / Metadata manager)
        ExecutionService exec = mock(ExecutionService.class);
        ManagedExecutorService executor = mock(ManagedExecutorService.class);
        when(exec.getExecutor(anyString())).thenReturn(executor);
        when(engine.getExecutionService()).thenReturn(exec);

        doReturn(mock(ILogger.class)).when(engine).getLogger(any(Class.class));

        return engine;
    }
}
