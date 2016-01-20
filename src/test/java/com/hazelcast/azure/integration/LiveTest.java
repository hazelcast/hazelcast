package com.hazelcast.azure.integration;

import com.hazelcast.azure.AzureDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.HazelcastTestSupport;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LiveTest extends HazelcastTestSupport {

    public static final String CLIENT_ID   =  System.getProperty("test.azure.client-id");
    public static final String CLIENT_SECRET =  System.getProperty("test.azure.client-secret");
    public static final String TENANT_ID =  System.getProperty("test.azure.tenant-id");
    public static final String SUBSCRIPTION_ID =  System.getProperty("test.azure.subscription-id");
    public static final String GROUP_NAME = System.getProperty("test.azure.group-name");
    public static final String HZLCST_CLUSTER_ID = System.getProperty("test.azure.cluster-id");
    
    protected Map<String, Comparable> getProperties() {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("client-id", CLIENT_ID);
        properties.put("client-secret", CLIENT_SECRET);
        properties.put("tenant-id", TENANT_ID);
        properties.put("subscription-id", SUBSCRIPTION_ID);
        properties.put("hzlcst-cluster-id", HZLCST_CLUSTER_ID);
        properties.put("group-name", GROUP_NAME);

        return properties;
    }

    @Test
    public void test_DiscoveryStrategyDiscoverNodes() throws Exception {
        Map<String, Comparable> properties = getProperties();
        properties.put("group", GROUP_NAME);
        AzureDiscoveryStrategy strategy = new AzureDiscoveryStrategy(properties);
        strategy.start();

        Iterator<DiscoveryNode> nodes = strategy.discoverNodes().iterator();
        
        assertTrue(nodes != null);

        int count = 0;

        while(nodes.hasNext()) {
            count++;
            DiscoveryNode node = nodes.next();
            System.out.println("Found Node:");
            System.out.println(node.getPrivateAddress());
            System.out.println(node.getPublicAddress());
        }

        assertEquals(2, count);
    }
}
