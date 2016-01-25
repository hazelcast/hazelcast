package com.hazelcast.jclouds.integration;


import com.google.common.collect.Iterators;
import com.hazelcast.jclouds.JCloudsDiscoveryStrategy;
import org.junit.Test;

import java.util.Map;

import static com.hazelcast.jclouds.integration.LiveComputeServiceUtil.GROUP_NAME1;
import static com.hazelcast.jclouds.integration.LiveComputeServiceUtil.GROUP_NAME2;
import static com.hazelcast.jclouds.integration.LiveComputeServiceUtil.tag1;
import static com.hazelcast.jclouds.integration.LiveComputeServiceUtil.tag2;
import static com.hazelcast.jclouds.integration.LiveComputeServiceUtil.tag3;
import static org.junit.Assert.assertEquals;

public abstract class AbstractLiveTest {

    protected abstract Map<String,Comparable> getProperties();
    protected abstract String getRegion1();
    protected abstract String getRegion2();

    @Test
    public void test_DiscoveryStrategyFiltersNodesByGroup() throws Exception {
        Map<String, Comparable> properties1 = getProperties();
        properties1.put("group", GROUP_NAME1);
        JCloudsDiscoveryStrategy strategy1 = new JCloudsDiscoveryStrategy(properties1);
        strategy1.start();

        Map<String, Comparable> properties2 = getProperties();
        properties2.put("group", GROUP_NAME2);
        JCloudsDiscoveryStrategy strategy2 = new JCloudsDiscoveryStrategy(properties2);
        strategy2.start();

        assertEquals(3,
                Iterators.size(strategy1.discoverNodes().iterator()));
        assertEquals(2,
                Iterators.size(strategy2.discoverNodes().iterator()));
    }

    @Test
    public void test_DiscoveryStrategyFiltersNodesByRegion() throws Exception {

        Map<String, Comparable> properties1 = getProperties();
        properties1.put("group", GROUP_NAME1);
        properties1.put("regions", getRegion1());
        JCloudsDiscoveryStrategy strategy1 = new JCloudsDiscoveryStrategy(properties1);
        strategy1.start();

        Map<String, Comparable> properties2 = getProperties();
        properties2.put("group", GROUP_NAME2);
        properties2.put("regions",getRegion1()+ "," + getRegion2());
        JCloudsDiscoveryStrategy strategy2 = new JCloudsDiscoveryStrategy(properties2);
        strategy2.start();

        assertEquals(2,
                Iterators.size(strategy1.discoverNodes().iterator()));

        assertEquals(2,
                Iterators.size(strategy2.discoverNodes().iterator()));
    }

    @Test
    public void test_DiscoveryStrategyFiltersNodesByTags() throws Exception {
        Map<String, Comparable> properties1 = getProperties();
        properties1.put("tag-keys", tag1.getKey());
        properties1.put("tag-values", tag1.getValue());
        JCloudsDiscoveryStrategy strategy1 = new JCloudsDiscoveryStrategy(properties1);
        strategy1.start();

        Map<String, Comparable> properties2 = getProperties();
        properties2.put("tag-keys", tag2.getKey() + "," + tag3.getKey());
        properties2.put("tag-values", tag2.getValue() + "," + tag3.getValue());
        JCloudsDiscoveryStrategy strategy2 = new JCloudsDiscoveryStrategy(properties2);
        strategy2.start();

        assertEquals(3,
                Iterators.size(strategy1.discoverNodes().iterator()));

        assertEquals(2,
                Iterators.size(strategy2.discoverNodes().iterator()));
    }
}
