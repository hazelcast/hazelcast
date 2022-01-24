package com.hazelcast.GB18030;


import com.hazelcast.client.Client;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.GB18030.RandomIdeograph.generate;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BasicProgrammaticComplianceTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void test_keyValuePairs_areRandomIdeograph() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        Collection<Client> connectedClients = instance.getClientService().getConnectedClients();
        assertEquals(connectedClients.size(), 1);

        final String mapName = generate();
        IMap<String, String> rndMap = client.getMap(mapName);

        final String key = generate();
        final String value = generate();

        rndMap.put(key, value);
        final String actual = rndMap.get(key);
        assertEquals(value, actual);
    }

    @Test
    public void test_clusterName_isRandomIdeograph() {
        final int clusterSize = nextInt(2, 5);
        final int clientSize = nextInt(2, 5);
        final String clusterName = generate();

        Config memberConfig = new Config().setClusterName(clusterName);
        ClientConfig clientConfig = new ClientConfig().setClusterName(clusterName);

        Set<HazelcastInstance> members = IntStream.range(0, clusterSize).boxed()
                .map(it -> hazelcastFactory.newHazelcastInstance(memberConfig))
                .collect(Collectors.toSet());

        Set<HazelcastInstance> clients = IntStream.range(0, clientSize).boxed()
                .map(it -> hazelcastFactory.newHazelcastClient(clientConfig))
                .collect(Collectors.toSet());

        members.forEach(m -> assertEquals(clusterSize, m.getCluster().getMembers().size()));
        members.forEach(m -> assertEquals(clientSize, m.getClientService().getConnectedClients().size()));
        clients.forEach(c -> assertEquals(clusterSize, c.getCluster().getMembers().size()));
    }
}
