/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.Endpoint;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.RaftUtil;
import com.hazelcast.test.AssertTask;
import com.hazelcast.util.function.Function;
import org.junit.Assert;

import java.util.Arrays;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getTerm;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.majority;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.minority;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.newRaftMember;
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

    private final CPGroupId groupId;
    private final RaftAlgorithmConfig raftAlgorithmConfig;
    private final String serviceName;
    private final Class<? extends SnapshotAwareService> serviceClazz;
    private final boolean appendNopEntryOnLeaderElection;
    private Endpoint[] initialMembers;
    private Endpoint[] members;
    private LocalRaftIntegration[] integrations;
    private RaftNodeImpl[] nodes;
    private int createdNodeCount;

    public LocalRaftGroup(int size) {
        this(size, new RaftAlgorithmConfig());
    }

    public LocalRaftGroup(int size, RaftAlgorithmConfig raftAlgorithmConfig) {
        this(size, raftAlgorithmConfig,  null, null, false);
    }

    public LocalRaftGroup(int size, RaftAlgorithmConfig raftAlgorithmConfig,
                          String serviceName, Class<? extends SnapshotAwareService> serviceClazz,
                          boolean appendNopEntryOnLeaderElection) {
        initialMembers = new Endpoint[size];
        members = new Endpoint[size];
        integrations = new LocalRaftIntegration[size];
        groupId = new TestRaftGroupId("test");
        this.raftAlgorithmConfig = raftAlgorithmConfig;
        this.serviceName = serviceName;
        this.serviceClazz = serviceClazz;
        this.appendNopEntryOnLeaderElection = appendNopEntryOnLeaderElection;

        for (; createdNodeCount < size; createdNodeCount++) {
            LocalRaftIntegration integration = createNewLocalRaftIntegration();
            integrations[createdNodeCount] = integration;
            initialMembers[createdNodeCount] = integration.getLocalEndpoint();
            members[createdNodeCount] = integration.getLocalEndpoint();
        }

        nodes = new RaftNodeImpl[size];
        for (int i = 0; i < size; i++) {
            LocalRaftIntegration integration = integrations[i];
            nodes[i] = new RaftNodeImpl(groupId, members[i], asList(members), raftAlgorithmConfig, integration);
        }
    }

    private LocalRaftIntegration createNewLocalRaftIntegration() {
        TestRaftMember endpoint = newRaftMember(5000 + createdNodeCount);
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

    public void startWithoutDiscovery() {
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
                RaftNodeImpl node = nodes[i];
                if (!node.getLocalMember().equals(integration.getLocalEndpoint())) {
                    integration.discoverNode(node);
                }
            }
        }
    }

    public RaftNodeImpl createNewRaftNode() {
        int oldSize = this.integrations.length;
        int newSize = oldSize + 1;
        Endpoint[] endpoints = new Endpoint[newSize];
        LocalRaftIntegration[] integrations = new LocalRaftIntegration[newSize];
        RaftNodeImpl[] nodes = new RaftNodeImpl[newSize];
        System.arraycopy(this.members, 0, endpoints, 0, oldSize);
        System.arraycopy(this.integrations, 0, integrations, 0, oldSize);
        System.arraycopy(this.nodes, 0, nodes, 0, oldSize);
        LocalRaftIntegration integration = createNewLocalRaftIntegration();
        createdNodeCount++;
        integrations[oldSize] = integration;
        Endpoint endpoint = integration.getLocalEndpoint();
        endpoints[oldSize] = endpoint;
        RaftNodeImpl node = new RaftNodeImpl(groupId, endpoint, asList(initialMembers), raftAlgorithmConfig, integration);
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

    public RaftNodeImpl[] getNodesExcept(Endpoint endpoint) {
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

    public RaftNodeImpl getNode(Endpoint endpoint) {
        return nodes[getIndexOf(endpoint)];
    }

    public Endpoint getEndpoint(int index) {
        return members[index];
    }

    public LocalRaftIntegration getIntegration(int index) {
        return integrations[index];
    }

    public LocalRaftIntegration getIntegration(Endpoint endpoint) {
        return getIntegration(getIndexOf(endpoint));
    }

    public <T extends SnapshotAwareService> T getService(Endpoint endpoint) {
        return getIntegration(getIndexOf(endpoint)).getService();
    }

    public <T extends SnapshotAwareService> T getService(RaftNodeImpl raftNode) {
        return getIntegration(getIndexOf(raftNode.getLocalMember())).getService();
    }

    public RaftNodeImpl waitUntilLeaderElected() {
        final RaftNodeImpl[] leaderRef = new RaftNodeImpl[1];
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                RaftNodeImpl leaderNode = getLeaderNode();
                assertNotNull(leaderNode);

                int leaderTerm = getTerm(leaderNode);

                for (RaftNodeImpl raftNode : nodes) {
                    if (integrations[getIndexOf(raftNode.getLocalMember())].isShutdown()) {
                        continue;
                    }

                    assertEquals(leaderNode.getLocalMember(), RaftUtil.getLeaderMember(raftNode));
                    assertEquals(leaderTerm, getTerm(raftNode));
                }

                leaderRef[0] = leaderNode;
            }
        });

        return leaderRef[0];
    }

    public Endpoint getLeaderEndpoint() {
        Endpoint leader = null;
        for (int i = 0; i < size(); i++) {
            if (integrations[i].isShutdown()) {
                continue;
            }
            RaftNodeImpl node = nodes[i];
            Endpoint endpoint = RaftUtil.getLeaderMember(node);
            if (leader == null) {
                leader = endpoint;
            } else if (!leader.equals(endpoint)) {
                throw new AssertionError("Group doesn't have a single leader endpoint yet!");
            }
        }
        return leader;
    }

    public RaftNodeImpl getLeaderNode() {
        Endpoint leaderEndpoint = getLeaderEndpoint();
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
        Endpoint leaderEndpoint = getLeaderEndpoint();
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
        Endpoint leaderEndpoint = getLeaderEndpoint();
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

    public int getIndexOf(Endpoint endpoint) {
        Assert.assertNotNull(endpoint);
        for (int i = 0; i < members.length; i++) {
            if (endpoint.equals(members[i])) {
                return i;
            }
        }
        throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
    }

    public void destroy() {
        for (LocalRaftIntegration integration : integrations) {
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

        int[] firstSplit = new int[size() - indexes.length];
        int[] secondSplit = indexes;

        int ix = 0;
        for (int i = 0; i < size(); i++) {
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
    public void split(Endpoint...endpoints) {
        int[] indexes = new int[endpoints.length];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = getIndexOf(endpoints[i]);
        }
        split(indexes);
    }

    public int[] createMinoritySplitIndexes(boolean includingLeader) {
        return createSplitIndexes(includingLeader, minority(size()));
    }

    public int[] createMajoritySplitIndexes(boolean includingLeader) {
        return createSplitIndexes(includingLeader, majority(size()));
    }

    public int[] createSplitIndexes(boolean includingLeader, int splitSize) {
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
    public void dropMessagesToMember(Endpoint from, Endpoint to, Class messageType) {
        getIntegration(getIndexOf(from)).dropMessagesToEndpoint(to, messageType);
    }

    /**
     * Allows specific message type one-way between from -> to.
     */
    public void allowMessagesToMember(Endpoint from, Endpoint to, Class messageType) {
        LocalRaftIntegration integration = getIntegration(getIndexOf(from));
        if (!integration.isReachable(to)) {
            throw new IllegalStateException("Cannot allow " + messageType + " from " + from
                    + " -> " + to + ", since all messages are dropped between.");
        }
        integration.allowMessagesToEndpoint(to, messageType);
    }

    /**
     * Drops all kind of messages one-way between from -> to.
     */
    public void dropAllMessagesToMember(Endpoint from, Endpoint to) {
        getIntegration(getIndexOf(from)).removeNode(getNode(getIndexOf(to)));
    }

    /**
     * Allows all kind of messages one-way between from -> to.
     */
    public void allowAllMessagesToMember(Endpoint from, Endpoint to) {
        LocalRaftIntegration integration = getIntegration(getIndexOf(from));
        integration.allowAllMessagesToEndpoint(to);
        integration.discoverNode(getNode(getIndexOf(to)));
    }

    /**
     * Drops specific message type one-way from -> to all nodes.
     */
    public void dropMessagesToAll(Endpoint from, Class messageType) {
        getIntegration(getIndexOf(from)).dropMessagesToAll(messageType);
    }

    /**
     * Allows specific message type one-way from -> to all nodes.
     */
    public void allowMessagesToAll(Endpoint from, Class messageType) {
        LocalRaftIntegration integration = getIntegration(getIndexOf(from));
        for (Endpoint endpoint : members) {
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
    public void resetAllRulesFrom(Endpoint endpoint) {
        getIntegration(getIndexOf(endpoint)).resetAllRules();
    }

    public void alterMessagesToMember(Endpoint from, Endpoint to, Function<Object, Object> function) {
        getIntegration(getIndexOf(from)).alterMessagesToEndpoint(to, function);
    }

    void removeAlterMessageRuleToMember(Endpoint from, Endpoint to) {
        getIntegration(getIndexOf(from)).removeAlterMessageRuleToEndpoint(to);
    }

    public void terminateNode(int index) {
        split(index);
        getIntegration(index).shutdown();
    }

    public void terminateNode(Endpoint endpoint) {
        terminateNode(getIndexOf(endpoint));
    }
}
