package com.hazelcast.GB18030;


import com.hazelcast.client.Client;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.config.YamlClientConfigBuilder;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.config.YamlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;

import static com.hazelcast.core.HazelcastTest.HAZELCAST_CONFIG;
import static com.hazelcast.test.HazelcastTestSupport.assertContains;
import static com.hazelcast.test.HazelcastTestSupport.assertNotContains;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BasicDeclarativeComplianceTest {
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void test_xmlConfigCompliance() throws IOException {
        URL schemaResource =
                BasicDeclarativeComplianceTest.class.getClassLoader().getResource("test-GB18030-hazelcast.xml");
        Config config = new XmlConfigBuilder(schemaResource).build();
        HazelcastInstance hz = hazelcastFactory.newHazelcastInstance(config);
        assertEquals("富吧", hz.getConfig().getClusterName());
        assertEquals(2, hz.getConfig().getMapConfig("默认").getBackupCount());
    }

    @Test
    public void test_xmlClientConfigCompliance() throws IOException {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();

        URL schemaResource = BasicDeclarativeComplianceTest.class.getClassLoader()
                .getResource("test-GB18030-hazelcast-client.xml");
        ClientConfig config = new XmlClientConfigBuilder(schemaResource).build();

        hazelcastFactory.newHazelcastClient(config);

        Collection<Client> connectedClients = instance.getClientService().getConnectedClients();
        Client client = connectedClients.iterator().next();

        assertEquals(1, client.getLabels().size());
        assertContains(client.getLabels(), "标签");
    }

    @Test
    public void test_yamlConfigCompliance() throws IOException {
        URL schemaResource = BasicDeclarativeComplianceTest.class.getClassLoader().getResource("test-GB18030-hazelcast.yaml");
        Config config = new YamlConfigBuilder(schemaResource).build();
        HazelcastInstance hz = hazelcastFactory.newHazelcastInstance(config);
        assertEquals("富吧", hz.getConfig().getClusterName());
        assertEquals(2, hz.getConfig().getMapConfig("默认").getBackupCount());
    }

    @Test
    public void test_yamlClientConfigCompliance() throws IOException {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();

        URL schemaResource = BasicDeclarativeComplianceTest.class.getClassLoader()
                .getResource("test-GB18030-hazelcast-client.yaml");
        ClientConfig config = new YamlClientConfigBuilder(schemaResource).build();

        hazelcastFactory.newHazelcastClient(config);

        Collection<Client> connectedClients = instance.getClientService().getConnectedClients();
        Client client = connectedClients.iterator().next();

        assertEquals(1, client.getLabels().size());
        assertContains(client.getLabels(), "标签");
    }
}
