package com.hazelcast;

import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

        DiscoveryConfig discoveryConfig = joinConfig.getDiscoveryConfig();
        assertTrue(discoveryConfig.isEnabled());

        assertEquals(1, discoveryConfig.getDiscoveryStrategyConfigs().size());

        DiscoveryStrategyConfig providerConfig = discoveryConfig.getDiscoveryStrategyConfigs().iterator().next();

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
}
