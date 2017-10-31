package com.hazelcast.raft.impl.testing;

import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftUtil;
import com.hazelcast.test.AssertTask;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.raft.impl.RaftUtil.getTerm;
import static com.hazelcast.raft.impl.RaftUtil.majority;
import static com.hazelcast.raft.impl.RaftUtil.minority;
import static com.hazelcast.raft.impl.RaftUtil.newRaftEndpoint;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableMap;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class RaftGroup {

    private final RaftEndpoint[] endpoints;
    private final LocalRaftIntegration[] integrations;
    private final RaftNode[] nodes;

    public RaftGroup(int size) {
        this(size, Collections.<String, Class>emptyMap());
    }

    public RaftGroup(int size, Map<String, Class> serviceRegistrations) {
        endpoints = new RaftEndpoint[size];
        integrations = new LocalRaftIntegration[size];

        for (int i = 0; i < size; i++) {
            endpoints[i] = newRaftEndpoint(5000 + i);
            Map<String, Object> services = new HashMap<String, Object>(serviceRegistrations.size());
            for (Map.Entry<String, Class> entry : serviceRegistrations.entrySet()) {
                try {
                    services.put(entry.getKey(), entry.getValue().newInstance());
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }
            integrations[i] = new LocalRaftIntegration(endpoints[i], unmodifiableMap(services));
        }

        nodes = new RaftNode[size];
        for (int i = 0; i < size; i++) {
            LocalRaftIntegration integration = integrations[i];
            nodes[i] = new RaftNode("node-" + i, endpoints[i], asList(endpoints), integration, integration.getStripedExecutor());
        }
    }

    public void start() {
        startWithoutDiscovery();
        initDiscovery();
    }

    public void startWithoutDiscovery() {
        for (RaftNode node : nodes) {
            node.start();
        }
    }

    public void initDiscovery() {
        for (LocalRaftIntegration integration : integrations) {
            for (int i = 0; i < size(); i++) {
                if (!integrations[i].isAlive()) {
                    continue;
                }
                RaftNode node = nodes[i];
                if (!node.getLocalEndpoint().equals(integration.getLocalEndpoint())) {
                    integration.discoverNode(node);
                }
            }
        }
    }

    public RaftNode[] getNodes() {
        return nodes;
    }

    public RaftNode[] getNodesExcept(RaftEndpoint endpoint) {
        RaftNode[] n = new RaftNode[nodes.length - 1];
        int i = 0;
        for (RaftNode node : nodes) {
            if (!node.getLocalEndpoint().equals(endpoint)) {
                n[i++] = node;
            }
        }

        if (i != n.length) {
            throw new IllegalArgumentException();
        }

        return n;
    }

    public RaftNode getNode(int index) {
        return nodes[index];
    }

    public RaftNode getNode(RaftEndpoint endpoint) {
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

    public <T> T getService(RaftEndpoint endpoint, String serviceName) {
        return getIntegration(getIndexOf(endpoint)).getService(serviceName);
    }

    public <T> T getService(RaftNode raftNode, String serviceName) {
        return getIntegration(getIndexOf(raftNode.getLocalEndpoint())).getService(serviceName);
    }

    public RaftNode waitUntilLeaderElected() {
        final RaftNode[] leaderRef = new RaftNode[1];
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                RaftNode leaderNode = getLeaderNode();
                assertNotNull(leaderNode);

                int leaderTerm = getTerm(leaderNode);

                for (RaftNode raftNode : nodes) {
                    if (!integrations[getIndexOf(raftNode.getLocalEndpoint())].isAlive()) {
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
            if (!integrations[i].isAlive()) {
                continue;
            }
            RaftNode node = nodes[i];
            RaftEndpoint endpoint = RaftUtil.getLeaderEndpoint(node);
            if (leader == null) {
                leader = endpoint;
            } else if (!leader.equals(endpoint)) {
                throw new AssertionError("Group doesn't have a single leader endpoint yet!");
            }
        }
        return leader;
    }

    public RaftNode getLeaderNode() {
        RaftEndpoint leaderEndpoint = getLeaderEndpoint();
        if (leaderEndpoint == null) {
            return null;
        }
        for (int i = 0; i < size(); i++) {
            if (!integrations[i].isAlive()) {
                continue;
            }
            RaftNode node = nodes[i];
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

        System.err.println("Creating splits. firstSplit = " + Arrays.toString(firstSplit)
            + ", secondSplit = " + Arrays.toString(secondSplit));
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
