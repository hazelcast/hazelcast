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

package com.hazelcast.cp.internal.raft.impl.testing;

import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.dataservice.RaftDataService;
import com.hazelcast.cp.internal.raft.impl.persistence.NopRaftStateStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateLoader;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;
import com.hazelcast.cp.internal.raft.impl.persistence.RestoredRaftState;
import com.hazelcast.function.BiFunctionEx;
import org.junit.Assert;

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.IntFunction;

import static com.hazelcast.cp.internal.raft.impl.RaftNodeImpl.newRaftNode;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeImpl.restoreRaftNode;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getTerm;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.majority;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.minority;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.newRaftMember;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Represents a local single Raft group, provides methods to access specific nodes, to terminate nodes,
 * to split/merge the group and to define allow/drop rules between nodes.
 */
public class LocalRaftGroup {

    public static class LocalRaftGroupBuilder {

        private int nodeCount;
        private RaftAlgorithmConfig config;
        private boolean appendNopEntryOnLeaderElection;
        private IntFunction<TestRaftEndpoint> endpointFactory;
        private BiFunctionEx<RaftEndpoint, RaftAlgorithmConfig, RaftStateStore> raftStateStoreFactory =
                (BiFunctionEx<RaftEndpoint, RaftAlgorithmConfig, RaftStateStore>) (endpoint, config) -> NopRaftStateStore.INSTANCE;
        private BiFunctionEx<RaftEndpoint, RaftAlgorithmConfig, RaftStateLoader> raftStateLoaderFactory;

        public LocalRaftGroupBuilder(int nodeCount) {
            this(nodeCount, new RaftAlgorithmConfig());
        }

        public LocalRaftGroupBuilder(int nodeCount, RaftAlgorithmConfig config) {
            this.nodeCount = nodeCount;
            this.config = config;
        }

        public LocalRaftGroupBuilder setAppendNopEntryOnLeaderElection(boolean appendNopEntryOnLeaderElection) {
            this.appendNopEntryOnLeaderElection = appendNopEntryOnLeaderElection;
            return this;
        }

        public LocalRaftGroupBuilder setRaftStateStoreFactory(BiFunctionEx<RaftEndpoint, RaftAlgorithmConfig, RaftStateStore> raftStateStoreFactory) {
            this.raftStateStoreFactory = raftStateStoreFactory;
            return this;
        }

        public LocalRaftGroupBuilder setRaftStateLoaderFactory(BiFunctionEx<RaftEndpoint, RaftAlgorithmConfig, RaftStateLoader> raftStateLoaderFactory) {
            this.raftStateLoaderFactory = raftStateLoaderFactory;
            return this;
        }

        public LocalRaftGroupBuilder setEndpointFactory(IntFunction<TestRaftEndpoint> endpointFactory) {
            this.endpointFactory = endpointFactory;
            return this;
        }

        public LocalRaftGroup build() {
            return new LocalRaftGroup(nodeCount, config, RaftDataService.SERVICE_NAME, RaftDataService.class,
                    appendNopEntryOnLeaderElection, endpointFactory, raftStateStoreFactory, raftStateLoaderFactory);
        }

        public static LocalRaftGroup newGroup(int nodeCount) {
            return new LocalRaftGroupBuilder(nodeCount).build();
        }

        public static LocalRaftGroup newGroup(int nodeCount, RaftAlgorithmConfig config) {
            return new LocalRaftGroupBuilder(nodeCount, config).build();
        }
    }

    private static final int FIRST_RAFT_NODE_PORT = 5000;


    private final CPGroupId groupId;
    private final RaftAlgorithmConfig raftAlgorithmConfig;
    private final String serviceName;
    private final Class<? extends SnapshotAwareService> serviceClazz;
    private final boolean appendNopEntryOnLeaderElection;
    private RaftEndpoint[] initialMembers;
    private RaftEndpoint[] members;
    private LocalRaftIntegration[] integrations;
    private RaftNodeImpl[] nodes;
    private int createdNodeCount;
    private BiFunctionEx<RaftEndpoint, RaftAlgorithmConfig, RaftStateStore> raftStateStoreFactory;
    private IntFunction<TestRaftEndpoint> endpointFactory = port -> newRaftMember(port);

    public LocalRaftGroup(int size) {
        this(size, new RaftAlgorithmConfig());
    }

    public LocalRaftGroup(int size, RaftAlgorithmConfig raftAlgorithmConfig) {
        this(size, raftAlgorithmConfig,  null, null, false);
    }

    public LocalRaftGroup(int size, RaftAlgorithmConfig raftAlgorithmConfig,
                          String serviceName, Class<? extends SnapshotAwareService> serviceClazz,
                          boolean appendNopEntryOnLeaderElection) {
        this(size, raftAlgorithmConfig, serviceName, serviceClazz, appendNopEntryOnLeaderElection, null, null, null);
    }

    public LocalRaftGroup(int size, RaftAlgorithmConfig raftAlgorithmConfig, String serviceName,
                          Class<? extends SnapshotAwareService> serviceClazz, boolean appendNopEntryOnLeaderElection,
                          IntFunction<TestRaftEndpoint> endpointFactory,
                          BiFunctionEx<RaftEndpoint, RaftAlgorithmConfig, RaftStateStore> raftStateStoreFactory,
                          BiFunctionEx<RaftEndpoint, RaftAlgorithmConfig, RaftStateLoader> raftStateLoaderFactory) {
        initialMembers = new RaftEndpoint[size];
        members = new RaftEndpoint[size];
        integrations = new LocalRaftIntegration[size];
        groupId = new TestRaftGroupId("test");
        this.raftAlgorithmConfig = raftAlgorithmConfig;
        this.serviceName = serviceName;
        this.serviceClazz = serviceClazz;
        this.appendNopEntryOnLeaderElection = appendNopEntryOnLeaderElection;

        if (endpointFactory != null) {
            this.endpointFactory = endpointFactory;
        }

        this.raftStateStoreFactory = raftStateStoreFactory;

        for (; createdNodeCount < size; createdNodeCount++) {
            LocalRaftIntegration integration = createNewLocalRaftIntegration();
            integrations[createdNodeCount] = integration;
            initialMembers[createdNodeCount] = integration.getLocalEndpoint();
            members[createdNodeCount] = integration.getLocalEndpoint();
        }

        nodes = new RaftNodeImpl[size];
        for (int i = 0; i < size; i++) {
            LocalRaftIntegration integration = integrations[i];

            if (raftStateStoreFactory == null) {
                if (raftStateLoaderFactory != null) {
                    try {
                        RaftStateLoader loader = raftStateLoaderFactory.apply(members[i], raftAlgorithmConfig);
                        RestoredRaftState state = loader.load();
                        assertNotNull(state);
                        nodes[i] = restoreRaftNode(groupId, state, raftAlgorithmConfig, integration);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    nodes[i] = newRaftNode(groupId,  members[i], asList(members), raftAlgorithmConfig, integration);
                }

            } else {
                RaftStateStore raftStateStore = raftStateStoreFactory.apply(members[i], raftAlgorithmConfig);
                if (raftStateLoaderFactory != null) {
                    try {
                        RaftStateLoader loader = raftStateLoaderFactory.apply(members[i], raftAlgorithmConfig);
                        RestoredRaftState state = loader.load();
                        assertNotNull(state);
                        nodes[i] = restoreRaftNode(groupId, state, raftAlgorithmConfig, integration, raftStateStore);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    nodes[i] = newRaftNode(groupId, members[i], asList(members), raftAlgorithmConfig, integration, raftStateStore);
                }
            }
        }

    }

    private LocalRaftIntegration createNewLocalRaftIntegration() {
        TestRaftEndpoint endpoint = endpointFactory.apply(FIRST_RAFT_NODE_PORT + createdNodeCount);
        return new LocalRaftIntegration(endpoint, groupId, createServiceInstance(), appendNopEntryOnLeaderElection);
    }

    private LocalRaftIntegration createNewLocalRaftIntegration(TestRaftEndpoint endpoint) {
        return new LocalRaftIntegration(endpoint, groupId, createServiceInstance(), appendNopEntryOnLeaderElection);
    }

    private SnapshotAwareService createServiceInstance() {
        if (serviceName != null && serviceClazz != null) {
            try {
                return serviceClazz.newInstance();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        return null;
    }

    public void start() {
        startWithoutDiscovery();
        initDiscovery();
    }

    private void startWithoutDiscovery() {
        for (RaftNodeImpl node : nodes) {
            node.start();
        }
    }

    private void initDiscovery() {
        for (LocalRaftIntegration integration : integrations) {
            for (int i = 0; i < size(); i++) {
                if (integrations[i].isShutdown()) {
                    continue;
                }
                RaftNodeImpl other = nodes[i];
                if (!other.getLocalMember().equals(integration.getLocalEndpoint())) {
                    integration.discoverNode(other);
                }
            }
        }
    }

    public RaftNodeImpl createNewRaftNode() {
        int oldSize = this.integrations.length;
        int newSize = oldSize + 1;
        RaftEndpoint[] endpoints = new RaftEndpoint[newSize];
        LocalRaftIntegration[] integrations = new LocalRaftIntegration[newSize];
        RaftNodeImpl[] nodes = new RaftNodeImpl[newSize];
        System.arraycopy(this.members, 0, endpoints, 0, oldSize);
        System.arraycopy(this.integrations, 0, integrations, 0, oldSize);
        System.arraycopy(this.nodes, 0, nodes, 0, oldSize);
        LocalRaftIntegration integration = createNewLocalRaftIntegration();
        createdNodeCount++;
        integrations[oldSize] = integration;
        RaftEndpoint endpoint = integration.getLocalEndpoint();
        endpoints[oldSize] = endpoint;
        RaftStateStore raftStateStore = raftStateStoreFactory.apply(endpoint, raftAlgorithmConfig);
        RaftNodeImpl node = newRaftNode(groupId, endpoint, asList(initialMembers), raftAlgorithmConfig, integration, raftStateStore);
        nodes[oldSize] = node;
        this.members = endpoints;
        this.integrations = integrations;
        this.nodes = nodes;

        node.start();
        initDiscovery();

        return node;
    }

    public RaftNodeImpl createNewRaftNode(RestoredRaftState restoredRaftState, RaftStateStore stateStore) {
        checkNotNull(restoredRaftState);
        int oldSize = this.integrations.length;
        int newSize = oldSize + 1;
        RaftEndpoint[] endpoints = new RaftEndpoint[newSize];
        LocalRaftIntegration[] integrations = new LocalRaftIntegration[newSize];
        RaftNodeImpl[] nodes = new RaftNodeImpl[newSize];
        System.arraycopy(this.members, 0, endpoints, 0, oldSize);
        System.arraycopy(this.integrations, 0, integrations, 0, oldSize);
        System.arraycopy(this.nodes, 0, nodes, 0, oldSize);
        LocalRaftIntegration integration = createNewLocalRaftIntegration((TestRaftEndpoint) restoredRaftState.localEndpoint());
        createdNodeCount++;
        integrations[oldSize] = integration;
        RaftEndpoint endpoint = integration.getLocalEndpoint();
        endpoints[oldSize] = endpoint;
        RaftNodeImpl node = restoreRaftNode(groupId, restoredRaftState, raftAlgorithmConfig, integration, stateStore);
        nodes[oldSize] = node;
        this.members = endpoints;
        this.integrations = integrations;
        this.nodes = nodes;

        node.start();
        initDiscovery();

        return node;
    }

    public RaftNodeImpl[] getNodes() {
        return nodes;
    }

    public RaftNodeImpl[] getNodesExcept(RaftEndpoint endpoint) {
        RaftNodeImpl[] n = new RaftNodeImpl[nodes.length - 1];
        int i = 0;
        for (RaftNodeImpl node : nodes) {
            if (!node.getLocalMember().equals(endpoint)) {
                n[i++] = node;
            }
        }

        if (i != n.length) {
            throw new IllegalArgumentException();
        }

        return n;
    }

    public RaftNodeImpl getNode(int index) {
        return nodes[index];
    }

    public RaftNodeImpl getNode(RaftEndpoint endpoint) {
        return nodes[getIndexOfRunning(endpoint)];
    }

    public RaftEndpoint[] getFollowerEndpoints() {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint();
        RaftEndpoint[] n = new RaftEndpoint[members.length - 1];
        int i = 0;
        for (RaftEndpoint member : members) {
            if (!member.equals(leaderEndpoint)) {
                n[i++] = member;
            }
        }

        if (i != n.length) {
            throw new IllegalArgumentException();
        }

        return n;
    }

    public RaftEndpoint getEndpoint(int index) {
        return members[index];
    }

    public LocalRaftIntegration getIntegration(int index) {
        return integrations[index];
    }

    public LocalRaftIntegration getIntegration(RaftEndpoint endpoint) {
        return getIntegration(getIndexOfRunning(endpoint));
    }

    public <T extends SnapshotAwareService> T getService(RaftEndpoint endpoint) {
        return getIntegration(getIndexOfRunning(endpoint)).getService();
    }

    public <T extends SnapshotAwareService> T getService(RaftNodeImpl raftNode) {
        return getIntegration(getIndexOfRunning(raftNode.getLocalMember())).getService();
    }

    public RaftNodeImpl waitUntilLeaderElected() {
        RaftNodeImpl[] leaderRef = new RaftNodeImpl[1];
        assertTrueEventually(() -> {
            RaftNodeImpl leaderNode = getLeaderNode();
            assertNotNull(leaderNode);

            int leaderTerm = getTerm(leaderNode);

            for (RaftNodeImpl raftNode : nodes) {
                if (integrations[getIndexOf(raftNode.getLocalMember())].isShutdown()) {
                    continue;
                }

                    assertEquals(leaderNode.getLocalMember(), getLeaderMember(raftNode));
                    assertEquals(leaderTerm, getTerm(raftNode));
                }

            leaderRef[0] = leaderNode;
        });

        return leaderRef[0];
    }

    public RaftEndpoint getLeaderEndpoint() {
        RaftEndpoint leader = null;
        for (int i = 0; i < size(); i++) {
            if (integrations[i].isShutdown()) {
                continue;
            }
            RaftNodeImpl node = nodes[i];
            RaftEndpoint endpoint = getLeaderMember(node);
            if (leader == null) {
                leader = endpoint;
            } else if (!leader.equals(endpoint)) {
                throw new AssertionError("Group doesn't have a single leader endpoint yet!");
            }
        }
        return leader;
    }

    public RaftNodeImpl getLeaderNode() {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint();
        if (leaderEndpoint == null) {
            return null;
        }
        for (int i = 0; i < size(); i++) {
            if (integrations[i].isShutdown()) {
                continue;
            }
            RaftNodeImpl node = nodes[i];
            if (leaderEndpoint.equals(node.getLocalMember())) {
                return node;
            }
        }
        throw new AssertionError("Leader endpoint is " + leaderEndpoint + ", but leader node could not be found!");
    }

    public int getLeaderIndex() {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint();
        if (leaderEndpoint == null) {
            return -1;
        }
        for (int i = 0; i < members.length; i++) {
            if (leaderEndpoint.equals(members[i])) {
                return i;
            }
        }
        throw new AssertionError("Leader endpoint is " + leaderEndpoint + ", but this endpoint is unknown to group!");
    }

    public RaftNodeImpl getAnyFollowerNode() {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint();
        if (leaderEndpoint == null) {
            throw new AssertionError("Group doesn't have a leader yet!");
        }
        for (int i = 0; i < size(); i++) {
            if (integrations[i].isShutdown()) {
                continue;
            }
            RaftNodeImpl node = nodes[i];
            if (!leaderEndpoint.equals(node.getLocalMember())) {
                return node;
            }
        }
        throw new AssertionError("There's no follower node available!");
    }

    private int getIndexOf(RaftEndpoint endpoint) {
        Assert.assertNotNull(endpoint);
        for (int i = 0; i < members.length; i++) {
            if (endpoint.equals(members[i])) {
                return i;
            }
        }
        throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
    }

    private int getIndexOfRunning(RaftEndpoint endpoint) {
        Assert.assertNotNull(endpoint);
        for (int i = 0; i < members.length; i++) {
            if (integrations[i].isShutdown()) {
                continue;
            }
            if (endpoint.equals(members[i])) {
                return i;
            }
        }
        throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
    }


    public void destroy() {
        for (int i = 0; i < nodes.length; i++) {
            RaftNodeImpl node = nodes[i];
            LocalRaftIntegration integration = integrations[i];
            if (integration.isShutdown()) {
                continue;
            }
            node.forceSetTerminatedStatus().joinInternal();
            integration.shutdown();
        }
    }

    public int size() {
        return members.length;
    }

    /**
     * Split nodes with these indexes from rest of the cluster.
     */
    public void split(int... indexes) {
        assertThat(indexes.length, greaterThan(0));
        assertThat(indexes.length, lessThan(size()));
        Arrays.sort(indexes);

        int runningMemberCount = 0;
        for (int i = 0; i < size(); i++) {
            if (getIntegration(i).isShutdown()) {
                continue;
            }
            runningMemberCount++;
        }

        int[] firstSplit = new int[runningMemberCount - indexes.length];
        int[] secondSplit = indexes;

        if (firstSplit.length == 0) {
            return;
        }

        int ix = 0;
        for (int i = 0; i < size(); i++) {
            if (getIntegration(i).isShutdown()) {
                continue;
            }
            if (Arrays.binarySearch(indexes, i) < 0) {
                firstSplit[ix++] = i;
            }
        }

        split(secondSplit, firstSplit);
        split(firstSplit, secondSplit);
    }

    private void split(int[] firstSplit, int[] secondSplit) {
        for (int i : firstSplit) {
            for (int j : secondSplit) {
                integrations[i].removeNode(nodes[j]);
            }
        }
    }

    /**
     * Split nodes having these members from rest of the cluster.
     */
    public void split(RaftEndpoint... endpoints) {
        int[] indexes = new int[endpoints.length];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = getIndexOfRunning(endpoints[i]);
        }
        split(indexes);
    }

    public int[] createMinoritySplitIndexes(boolean includingLeader) {
        return createSplitIndexes(includingLeader, minority(size()));
    }

    public int[] createMajoritySplitIndexes(boolean includingLeader) {
        return createSplitIndexes(includingLeader, majority(size()));
    }

    private int[] createSplitIndexes(boolean includingLeader, int splitSize) {
        int leader = getLeaderIndex();

        int[] indexes = new int[splitSize];
        int ix = 0;

        if (includingLeader) {
            indexes[0] = leader;
            ix = 1;
        }

        for (int i = 0; i < size(); i++) {
            if (i == leader) {
                continue;
            }
            if (ix == indexes.length) {
                break;
            }
            indexes[ix++] = i;
        }
        return indexes;
    }

    public void merge() {
        initDiscovery();
    }

    /**
     * Drops specific message type one-way between from -> to.
     */
    public void dropMessagesToMember(RaftEndpoint from, RaftEndpoint to, Class messageType) {
        getIntegration(getIndexOfRunning(from)).dropMessagesToEndpoint(to, messageType);
    }

    /**
     * Allows specific message type one-way between from -> to.
     */
    public void allowMessagesToMember(RaftEndpoint from, RaftEndpoint to, Class messageType) {
        LocalRaftIntegration integration = getIntegration(getIndexOfRunning(from));
        if (!integration.isReachable(to)) {
            throw new IllegalStateException("Cannot allow " + messageType + " from " + from
                    + " -> " + to + ", since all messages are dropped between.");
        }
        integration.allowMessagesToEndpoint(to, messageType);
    }

    /**
     * Drops all kind of messages one-way between from -> to.
     */
    public void dropAllMessagesToMember(RaftEndpoint from, RaftEndpoint to) {
        getIntegration(getIndexOf(from)).removeNode(getNode(getIndexOfRunning(to)));
    }

    /**
     * Allows all kind of messages one-way between from -> to.
     */
    public void allowAllMessagesToMember(RaftEndpoint from, RaftEndpoint to) {
        LocalRaftIntegration integration = getIntegration(getIndexOfRunning(from));
        integration.allowAllMessagesToEndpoint(to);
        integration.discoverNode(getNode(getIndexOfRunning(to)));
    }

    /**
     * Drops specific message type one-way from -> to all nodes.
     */
    public void dropMessagesToAll(RaftEndpoint from, Class messageType) {
        getIntegration(getIndexOfRunning(from)).dropMessagesToAll(messageType);
    }

    /**
     * Allows specific message type one-way from -> to all nodes.
     */
    public void allowMessagesToAll(RaftEndpoint from, Class messageType) {
        LocalRaftIntegration integration = getIntegration(getIndexOfRunning(from));
        for (RaftEndpoint endpoint : members) {
            if (!integration.isReachable(endpoint)) {
                throw new IllegalStateException("Cannot allow " + messageType + " from " + from
                        + " -> " + endpoint + ", since all messages are dropped between.");
            }
        }
        integration.allowMessagesToAll(messageType);
    }

    /**
     * Resets all rules from endpoint.
     */
    public void resetAllRulesFrom(RaftEndpoint endpoint) {
        getIntegration(getIndexOfRunning(endpoint)).resetAllRules();
    }

    public void alterMessagesToMember(RaftEndpoint from, RaftEndpoint to, Function<Object, Object> function) {
        getIntegration(getIndexOfRunning(from)).alterMessagesToEndpoint(to, function);
    }

    void removeAlterMessageRuleToMember(RaftEndpoint from, RaftEndpoint to) {
        getIntegration(getIndexOfRunning(from)).removeAlterMessageRuleToEndpoint(to);
    }

    public void terminateNode(int index) {
        split(index);
        LocalRaftIntegration integration = getIntegration(index);
        if (!integration.isShutdown()) {
            getNode(index).forceSetTerminatedStatus().joinInternal();
            integration.shutdown();
        }
    }

    public void terminateNode(RaftEndpoint endpoint) {
        terminateNode(getIndexOfRunning(endpoint));
    }

    public boolean isRunning(RaftEndpoint endpoint) {
        Assert.assertNotNull(endpoint);
        for (int i = 0; i < members.length; i++) {
            if (!endpoint.equals(members[i])) {
                continue;
            }
            return !integrations[i].isShutdown();
        }
        throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
    }
}
