package com.hazelcast.raft.impl.testing;

import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.RaftUtil;
import com.hazelcast.test.AssertTask;
import org.junit.Assert;

import java.util.Arrays;

import static com.hazelcast.raft.impl.RaftUtil.getTerm;
import static com.hazelcast.raft.impl.RaftUtil.majority;
import static com.hazelcast.raft.impl.RaftUtil.minority;
import static com.hazelcast.raft.impl.RaftUtil.newRaftEndpoint;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
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

    private final RaftGroupId groupId;
    private final RaftConfig raftConfig;
    private final String serviceName;
    private final Class<? extends SnapshotAwareService> serviceClazz;
    private RaftEndpoint[] endpoints;
    private LocalRaftIntegration[] integrations;
    private RaftNodeImpl[] nodes;
    private int createdNodeCount;

    public LocalRaftGroup(int size) {
        this(size, new RaftConfig());
    }

    public LocalRaftGroup(int size, RaftConfig raftConfig) {
        this(size,raftConfig,  null, null);
    }

    public LocalRaftGroup(int size, RaftConfig raftConfig, String serviceName, Class<? extends SnapshotAwareService> serviceClazz) {
        endpoints = new RaftEndpoint[size];
        integrations = new LocalRaftIntegration[size];
        this.raftConfig = raftConfig;
        this.serviceName = serviceName;
        this.serviceClazz = serviceClazz;

        for (; createdNodeCount < size; createdNodeCount++) {
            LocalRaftIntegration integration = createNewLocalRaftIntegration();
            integrations[createdNodeCount] = integration;
            endpoints[createdNodeCount] = integration.getLocalEndpoint();
        }

        nodes = new RaftNodeImpl[size];
        groupId = new RaftGroupIdImpl("test", 1);
        for (int i = 0; i < size; i++) {
            LocalRaftIntegration integration = integrations[i];
            nodes[i] = new RaftNodeImpl(serviceName, groupId, endpoints[i], asList(endpoints), raftConfig, integration);
        }
    }

    private LocalRaftIntegration createNewLocalRaftIntegration() {
        return new LocalRaftIntegration(newRaftEndpoint(5000 + createdNodeCount), raftConfig, createServiceInstance());
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
                if (!node.getLocalEndpoint().equals(integration.getLocalEndpoint())) {
                    integration.discoverNode(node);
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
        System.arraycopy(this.endpoints, 0, endpoints, 0, oldSize);
        System.arraycopy(this.integrations, 0, integrations, 0, oldSize);
        System.arraycopy(this.nodes, 0, nodes, 0, oldSize);
        LocalRaftIntegration integration = createNewLocalRaftIntegration();
        integrations[oldSize] = integration;
        RaftEndpoint endpoint = integration.getLocalEndpoint();
        endpoints[oldSize] = endpoint;
        RaftNodeImpl node = new RaftNodeImpl(serviceName, groupId, endpoint, singletonList(endpoint), raftConfig, integration);
        nodes[oldSize] = node;
        this.endpoints = endpoints;
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
            if (!node.getLocalEndpoint().equals(endpoint)) {
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
        return nodes[getIndexOf(endpoint)];
    }

    public RaftEndpoint getEndpoint(int index) {
        return endpoints[index];
    }

    public LocalRaftIntegration getIntegration(int index) {
        return integrations[index];
    }

    public LocalRaftIntegration getIntegration(RaftEndpoint endpoint) {
        return getIntegration(getIndexOf(endpoint));
    }

    public <T extends SnapshotAwareService> T getService(RaftEndpoint endpoint) {
        return getIntegration(getIndexOf(endpoint)).getService();
    }

    public <T extends SnapshotAwareService> T getService(RaftNodeImpl raftNode) {
        return getIntegration(getIndexOf(raftNode.getLocalEndpoint())).getService();
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
                    if (integrations[getIndexOf(raftNode.getLocalEndpoint())].isShutdown()) {
                        continue;
                    }

                    assertEquals(leaderNode.getLocalEndpoint(), RaftUtil.getLeaderEndpoint(raftNode));
                    assertEquals(leaderTerm, getTerm(raftNode));
                }

                leaderRef[0] = leaderNode;
            }
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
            RaftEndpoint endpoint = RaftUtil.getLeaderEndpoint(node);
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
            if (leaderEndpoint.equals(node.getLocalEndpoint())) {
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
        for (int i = 0; i < endpoints.length; i++) {
            if (leaderEndpoint.equals(endpoints[i])) {
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
            if (!leaderEndpoint.equals(node.getLocalEndpoint())) {
                return node;
            }
        }
        throw new AssertionError("There's no follower node available!");
    }

    public int getIndexOf(RaftEndpoint endpoint) {
        Assert.assertNotNull(endpoint);
        for (int i = 0; i < endpoints.length; i++) {
            if (endpoint.equals(endpoints[i])) {
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
        return endpoints.length;
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
     * Split nodes having these endpoints from rest of the cluster.
     */
    public void split(RaftEndpoint...endpoints) {
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
    public void dropMessagesToEndpoint(RaftEndpoint from, RaftEndpoint to, Class messageType) {
        getIntegration(getIndexOf(from)).dropMessagesToEndpoint(to, messageType);
    }

    /**
     * Allows specific message type one-way between from -> to.
     */
    public void allowMessagesToEndpoint(RaftEndpoint from, RaftEndpoint to, Class messageType) {
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
    public void dropAllMessagesToEndpoint(RaftEndpoint from, RaftEndpoint to) {
        getIntegration(getIndexOf(from)).removeNode(getNode(getIndexOf(to)));
    }

    /**
     * Allows all kind of messages one-way between from -> to.
     */
    public void allowAllMessagesToEndpoint(RaftEndpoint from, RaftEndpoint to) {
        LocalRaftIntegration integration = getIntegration(getIndexOf(from));
        integration.allowAllMessagesToEndpoint(to);
        integration.discoverNode(getNode(getIndexOf(to)));
    }

    /**
     * Drops specific message type one-way from -> to all nodes.
     */
    public void dropMessagesToAll(RaftEndpoint from, Class messageType) {
        getIntegration(getIndexOf(from)).dropMessagesToAll(messageType);
    }

    /**
     * Allows specific message type one-way from -> to all nodes.
     */
    public void allowMessagesToAll(RaftEndpoint from, Class messageType) {
        LocalRaftIntegration integration = getIntegration(getIndexOf(from));
        for (RaftEndpoint endpoint : endpoints) {
            if (!integration.isReachable(endpoint)) {
                throw new IllegalStateException("Cannot allow " + messageType + " from " + from
                        + " -> " + endpoint + ", since all messages are dropped between.");
            }
        }
        integration.allowMessagesToAll(messageType);
    }

    /**
     * Resets all drop rules from endpoint.
     */
    public void resetAllDropRulesFrom(RaftEndpoint endpoint) {
        getIntegration(getIndexOf(endpoint)).resetAllDropRules();
    }

    public void terminateNode(int index) {
        split(index);
        getIntegration(index).shutdown();
    }

    public void terminateNode(RaftEndpoint endpoint) {
        terminateNode(getIndexOf(endpoint));
    }
}
