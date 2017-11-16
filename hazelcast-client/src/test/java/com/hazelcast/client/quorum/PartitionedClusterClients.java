package com.hazelcast.client.quorum;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.quorum.PartitionedCluster;

import static com.hazelcast.client.quorum.QuorumTestUtil.createClient;

public class PartitionedClusterClients {

    private TestHazelcastFactory factory;
    private HazelcastInstance clients[];
    private PartitionedCluster cluster;

    public PartitionedClusterClients(PartitionedCluster cluster, TestHazelcastFactory factory) {
        this.cluster = cluster;
        this.clients = new HazelcastInstance[cluster.instance.length];
        this.factory = factory;
    }

    public HazelcastInstance client(int index) {
        if (clients[index] == null) {
            clients[index] = createClient(factory, cluster.instance[index]);
        }
        return clients[index];
    }

    public void terminateAll() {
        for (HazelcastInstance client : clients) {
            if(client != null) {
                client.shutdown();
            }
        }
    }

}
