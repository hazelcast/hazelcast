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

package com.hazelcast.cp.internal;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.persistence.CPMetadataStore;
import com.hazelcast.cp.internal.persistence.CPPersistenceService;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.snapshot.NonChunkedSnapshotImmutabilityTest;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MetadataRaftGroupManagerSnapshotImmutabilityTest
        extends NonChunkedSnapshotImmutabilityTest<MetadataRaftGroupSnapshot, MetadataRaftGroupManager> {

    private RaftGroupId metadataGroupId;
    private RaftGroupId testGroupId;

    @Before
    public void setUp() throws Exception {
        NodeEngineImpl engine = mockNodeEngine();

        RaftService raftService = engine.getService(RaftService.SERVICE_NAME);
        configurePersistenceMocks(raftService);

        RaftGroupMembershipManager raftGroupMembershipManager =
                new RaftGroupMembershipManager(engine, raftService);

        MetadataRaftGroupManager manager =
                new MetadataRaftGroupManager(engine, raftService, engine.getConfig().getCPSubsystemConfig(),
                        raftGroupMembershipManager, new NoOpMembershipPolicy());

        this.metadataGroupId = manager.getMetadataGroupId();
        this.testGroupId = new RaftGroupId("testGroup", 1, 1);
    }

    @Override
    protected MetadataRaftGroupManager newService() {
        NodeEngineImpl engine = mockNodeEngine();
        RaftService raftService = engine.getService(RaftService.SERVICE_NAME);
        configurePersistenceMocks(raftService);
        RaftGroupMembershipManager raftGroupMembershipManager =
                new RaftGroupMembershipManager(engine, raftService);

        MetadataRaftGroupManager manager =
                new MetadataRaftGroupManager(engine, raftService, engine.getConfig().getCPSubsystemConfig(),
                        raftGroupMembershipManager, new NoOpMembershipPolicy());
        return manager;
    }

    @Override
    protected CPGroupId groupId() {
        return metadataGroupId;
    }

    @Override
    protected void populateInitialState(MetadataRaftGroupManager service, CPGroupId groupId) {
        MetadataRaftGroupSnapshot bootstrap = newSnapshotPayload();
        service.restoreSnapshot(groupId, 1L, bootstrap);
    }

    @Override
    protected void mutateService(MetadataRaftGroupManager service, CPGroupId groupId) {
        service.restart(1234L);
    }

    @Override
    protected Object snapshotFingerprint(MetadataRaftGroupSnapshot snapshot) {
        Map<String, Object> root = new TreeMap<>();

        // members
        Set<UUID> memberUuids = snapshot.getMembers().stream()
                .map(CPMemberInfo::getUuid)
                .collect(Collectors.toCollection(TreeSet::new));
        root.put("members", memberUuids);
        root.put("membersCommitIndex", snapshot.getMembersCommitIndex());

        // groups
        Map<String, Object> groupsFp = new TreeMap<>();
        for (CPGroupInfo group : snapshot.getGroups()) {
            Map<String, Object> g = new TreeMap<>();
            g.put("id", group.id().getId());
            g.put("status", group.status());
            g.put("memberCount", group.memberCount());
            groupsFp.put(group.name(), g);
        }
        root.put("groups", groupsFp);

        // initialization state
        root.put("initStatus", snapshot.getInitializationStatus());

        Set<UUID> initializedMemberUuids = snapshot.getInitializedCPMembers().stream()
                .map(CPMemberInfo::getUuid)
                .collect(Collectors.toCollection(TreeSet::new));
        root.put("initializedMembers", initializedMemberUuids);

        Set<Long> initCommitIndices = new TreeSet<>(snapshot.getInitializationCommitIndices());
        root.put("initCommitIndices", initCommitIndices);

        // membership change schedule
        MembershipChangeSchedule schedule = snapshot.getMembershipChangeSchedule();

        Map<String, Object> schedFp = new TreeMap<>();

        // commit indices (sorted)
        schedFp.put("commitIndices",
                new TreeSet<>(schedule.getMembershipChangeCommitIndices()));

        // added / leaving member UUIDs (if present)
        CPMemberInfo added = schedule.getAddedMember();
        CPMemberInfo leaving = schedule.getLeavingMember();
        schedFp.put("addedMemberUuid", added != null ? added.getUuid() : null);
        schedFp.put("leavingMemberUuid", leaving != null ? leaving.getUuid() : null);

        // changes: list of maps with a very small summary
        List<Map<String, Object>> changesFp = new ArrayList<>();
        for (MembershipChangeSchedule.CPGroupMembershipChange c : schedule.getChanges()) {
            Map<String, Object> cf = new TreeMap<>();
            cf.put("groupName", c.getGroupId().getName());
            cf.put("groupId", c.getGroupId().getId());
            cf.put("expectedMembersCommitIndex", c.getMembersCommitIndex());

            RaftEndpoint addEp = c.getMemberToAdd();
            RaftEndpoint rmEp = c.getMemberToRemove();
            cf.put("addUuid", addEp != null ? addEp.getUuid() : null);
            cf.put("removeUuid", rmEp != null ? rmEp.getUuid() : null);

            changesFp.add(cf);
        }
        schedFp.put("changes", changesFp);

        root.put("membershipSchedule", schedFp);

        return root;
    }

    private MetadataRaftGroupSnapshot newSnapshotPayload() {
        // Create one CP member
        UUID memberUuid = UUID.randomUUID();
        CPMemberInfo member = new CPMemberInfo(memberUuid, new Address(), false);

        Collection<CPMemberInfo> members = List.of(member);
        long membersCommitIndex = 42L;

        // Create a CP group containing that member
        RaftEndpoint ep = mock(RaftEndpoint.class);
        when(ep.getUuid()).thenReturn(memberUuid);

        CPGroupInfo groupInfo = new CPGroupInfo(testGroupId, List.of(ep));

        Collection<CPGroupInfo> groups = List.of(groupInfo);

        // Create a membership change for removing this member
        MembershipChangeSchedule.CPGroupMembershipChange change =
                new MembershipChangeSchedule.CPGroupMembershipChange(
                        testGroupId,
                        membersCommitIndex,
                        groupInfo.memberImpls(),
                        null, ep
                );

        MembershipChangeSchedule schedule =
                MembershipChangeSchedule.forLeavingMember(
                        List.of(99L),
                        member,
                        List.of(change)
                );

        // Initialization parameters
        List<CPMemberInfo> initialCPMembers = List.of(member);
        Set<CPMemberInfo> initializedCPMembers = new HashSet<>(List.of(member));
        MetadataRaftGroupManager.MetadataRaftGroupInitStatus initStatus =
                MetadataRaftGroupManager.MetadataRaftGroupInitStatus.SUCCESSFUL;
        Set<Long> initCommitIndices = new HashSet<>(List.of(10L));

        return new MetadataRaftGroupSnapshot(
                members,
                membersCommitIndex,
                groups,
                schedule,
                initialCPMembers,
                initializedCPMembers,
                initStatus,
                initCommitIndices
        );
    }

    @Override
    public NodeEngineImpl mockNodeEngine() {
        NodeEngineImpl engine = mock(NodeEngineImpl.class);

        // Config + CPSubsystemConfig
        Config cfg = new Config();
        CPSubsystemConfig cpCfg = new CPSubsystemConfig();
        cpCfg.setCPMemberCount(3);
        cfg.setCPSubsystemConfig(cpCfg);
        when(engine.getConfig()).thenReturn(cfg);

        // Properties
        HazelcastProperties props = new HazelcastProperties(new Properties());
        when(engine.getProperties()).thenReturn(props);

        // Metrics
        MetricsRegistry metrics = mock(MetricsRegistry.class);
        when(engine.getMetricsRegistry()).thenReturn(metrics);

        // Logger
        ILogger logger = mock(ILogger.class);
        when(engine.getLogger(any(Class.class))).thenReturn(logger);

        // ExecutionService + executor (used by RaftGroupMembershipManager / Metadata manager)
        ExecutionService exec = mock(ExecutionService.class);
        ManagedExecutorService executor = mock(ManagedExecutorService.class);
        // We don't care about the name; just always return some executor
        when(exec.getExecutor(anyString())).thenReturn(executor);
        when(engine.getExecutionService()).thenReturn(exec);

        // RaftService
        RaftService raftService = mock(RaftService.class);
        when(raftService.isCpSubsystemEnabled()).thenReturn(true);
        when(engine.getService(RaftService.SERVICE_NAME)).thenReturn(raftService);

        return engine;
    }

    /**
     * Extra CP persistence mocking needed by MetadataRaftGroupManager.
     */
    private void configurePersistenceMocks(RaftService raftService) {
        CPPersistenceService cpPersistenceService = mock(CPPersistenceService.class);
        CPMetadataStore metadataStore = mock(CPMetadataStore.class);

        when(raftService.getCPPersistenceService()).thenReturn(cpPersistenceService);
        when(cpPersistenceService.getCPMetadataStore()).thenReturn(metadataStore);

        try {
            doNothing().when(metadataStore).persistActiveCPMembers(anyCollection(), anyLong());
            doNothing().when(metadataStore).persistMetadataGroupId(any(RaftGroupId.class));
            doNothing().when(metadataStore).persistLocalCPMember(any(CPMemberInfo.class));
            when(metadataStore.containsLocalMemberFile()).thenReturn(false);
            when(metadataStore.isMarkedAPMember()).thenReturn(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
