package com.hazelcast;


import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryStrategiesConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class JCloudsDiscoveryFactoryTest extends HazelcastTestSupport {

    @Test
    public void testParsing() throws Exception {
        String xmlFileName = "test-jclouds-config.xml";
        InputStream xmlResource = JCloudsDiscoveryFactoryTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        Config config = new XmlConfigBuilder(xmlResource).build();

        JoinConfig joinConfig = config.getNetworkConfig().getJoin();

        AwsConfig awsConfig = joinConfig.getAwsConfig();
        assertFalse(awsConfig.isEnabled());

        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig();
        assertFalse(tcpIpConfig.isEnabled());

        MulticastConfig multicastConfig = joinConfig.getMulticastConfig();
        assertFalse(multicastConfig.isEnabled());

        DiscoveryStrategiesConfig discoveryStrategiesConfig = joinConfig.getDiscoveryStrategiesConfig();
        assertTrue(discoveryStrategiesConfig.isEnabled());

        assertEquals(1, discoveryStrategiesConfig.getDiscoveryStrategyConfigs().size());

        DiscoveryStrategyConfig providerConfig = discoveryStrategiesConfig.getDiscoveryStrategyConfigs().iterator().next();

        assertEquals(11, providerConfig.getProperties().size());
        assertEquals("aws-ec2", providerConfig.getProperties().get("provider"));
        assertEquals("test", providerConfig.getProperties().get("identity"));
        assertEquals("test", providerConfig.getProperties().get("credential"));
        assertEquals("zone1,zone2", providerConfig.getProperties().get("zones"));
        assertEquals("region1,region2", providerConfig.getProperties().get("regions"));
        assertEquals("zone1,zone2", providerConfig.getProperties().get("zones"));
        assertEquals("tag1,tag2", providerConfig.getProperties().get("tag-keys"));
        assertEquals("tagvalue1,tagvalue2", providerConfig.getProperties().get("tag-values"));
        assertEquals("group", providerConfig.getProperties().get("group"));
        assertEquals("5702", providerConfig.getProperties().get("hz-port"));
        assertEquals("myfile.json", providerConfig.getProperties().get("credentialPath"));
        assertEquals("myRole", providerConfig.getProperties().get("role-name"));
    }


    @Test
    public void testNodeStartup() {
        String xmlFileName = "test-jclouds-config.xml";
        InputStream xmlResource = JCloudsDiscoveryFactoryTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        Config config = new XmlConfigBuilder(xmlResource).build();
        config.getNetworkConfig().setPort(50001);
        InterfacesConfig interfaces = config.getNetworkConfig().getInterfaces();
        interfaces.clear();
        interfaces.setEnabled(true);
        interfaces.addInterface("127.0.0.1");

        String[] addresses = {"127.0.0.1", "127.0.0.1", "127.0.0.1"};
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(50001, addresses);

        final HazelcastInstance hazelcastInstance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance hazelcastInstance2 = factory.newHazelcastInstance(config);
        final HazelcastInstance hazelcastInstance3 = factory.newHazelcastInstance(config);

        assertNotNull(hazelcastInstance1);
        assertNotNull(hazelcastInstance2);
        assertNotNull(hazelcastInstance3);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {

                assertEquals(3, hazelcastInstance1.getCluster().getMembers().size());
                assertEquals(3, hazelcastInstance2.getCluster().getMembers().size());
                assertEquals(3, hazelcastInstance3.getCluster().getMembers().size());
            }
        });
    }
}
