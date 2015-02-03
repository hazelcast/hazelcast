/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring;

import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.MemberGroupConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.MapStoreFactory;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MultiMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.nio.ssl.SSLContextFactory;
import com.hazelcast.spring.serialization.DummyDataSerializableFactory;
import com.hazelcast.spring.serialization.DummyPortableFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanReplicationEndpoint;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"fullcacheconfig-applicationContext-hazelcast.xml"})
@Category(QuickTest.class)
public class TestFullApplicationContext {

    private Config config;

    @Resource(name = "instance")
    private HazelcastInstance instance;

    @Resource(name = "map1")
    private IMap<Object, Object> map1;

    @Resource(name = "map2")
    private IMap<Object, Object> map2;

    @Resource(name = "multiMap")
    private MultiMap multiMap;

    @Resource(name = "queue")
    private IQueue queue;

    @Resource(name = "topic")
    private ITopic topic;

    @Resource(name = "set")
    private ISet set;

    @Resource(name = "list")
    private IList list;

    @Resource(name = "executorService")
    private ExecutorService executorService;

    @Resource(name = "idGenerator")
    private IdGenerator idGenerator;

    @Resource(name = "atomicLong")
    private IAtomicLong atomicLong;

    @Resource(name = "atomicReference")
    private IAtomicReference atomicReference;

    @Resource(name = "countDownLatch")
    private ICountDownLatch countDownLatch;

    @Resource(name = "semaphore")
    private ISemaphore semaphore;

    @Resource(name = "lock")
    private ILock lock;

    @Resource(name = "dummyMapStore")
    private MapStore dummyMapStore;

    @Autowired
    private MapStoreFactory dummyMapStoreFactory;

    @Autowired
    private WanReplicationEndpoint wanReplication;

    @Autowired
    private MembershipListener membershipListener;

    @Autowired
    private EntryListener entryListener;

    @Resource
    private SSLContextFactory sslContextFactory;

    @Resource
    private SocketInterceptor socketInterceptor;

    @Resource
    private StreamSerializer dummySerializer;

    @BeforeClass
    @AfterClass
    public static void start() {
        Hazelcast.shutdownAll();
    }

    @Before
    public void before() {
        config = instance.getConfig();
    }

    @Test
    public void testMapConfig() {
        assertNotNull(config);
        assertEquals(9, config.getMapConfigs().size());

        MapConfig testMapConfig = config.getMapConfig("testMap");
        assertNotNull(testMapConfig);
        assertEquals("testMap", testMapConfig.getName());
        assertEquals(2, testMapConfig.getBackupCount());
        assertEquals(EvictionPolicy.NONE, testMapConfig.getEvictionPolicy());
        assertEquals(Integer.MAX_VALUE, testMapConfig.getMaxSizeConfig().getSize());
        assertEquals(30, testMapConfig.getEvictionPercentage());
        assertEquals(0, testMapConfig.getTimeToLiveSeconds());
        assertEquals(1000, testMapConfig.getMinEvictionCheckMillis());
        assertEquals("PUT_IF_ABSENT", testMapConfig.getMergePolicy());
        assertTrue(testMapConfig.isReadBackupData());
        assertEquals(2, testMapConfig.getMapIndexConfigs().size());
        for (MapIndexConfig index : testMapConfig.getMapIndexConfigs()) {
            if ("name".equals(index.getAttribute())) {
                assertFalse(index.isOrdered());
            } else if ("age".equals(index.getAttribute())) {
                assertTrue(index.isOrdered());
            } else {
                fail("unknown index!");
            }
        }

        // Test that the testMapConfig has a mapStoreConfig and it is correct
        MapStoreConfig testMapStoreConfig = testMapConfig.getMapStoreConfig();
        assertNotNull(testMapStoreConfig);
        assertEquals("com.hazelcast.spring.DummyStore", testMapStoreConfig.getClassName());
        assertTrue(testMapStoreConfig.isEnabled());
        assertEquals(0, testMapStoreConfig.getWriteDelaySeconds());
        assertEquals(10, testMapStoreConfig.getWriteBatchSize());
        assertTrue(testMapStoreConfig.isWriteCoalescing());
        assertEquals(MapStoreConfig.InitialLoadMode.EAGER, testMapStoreConfig.getInitialLoadMode());

        // Test that the testMapConfig has a nearCacheConfig and it is correct
        NearCacheConfig testNearCacheConfig = testMapConfig.getNearCacheConfig();
        assertNotNull(testNearCacheConfig);
        assertEquals(0, testNearCacheConfig.getTimeToLiveSeconds());
        assertEquals(60, testNearCacheConfig.getMaxIdleSeconds());
        assertEquals("LRU", testNearCacheConfig.getEvictionPolicy());
        assertEquals(5000, testNearCacheConfig.getMaxSize());
        assertTrue(testNearCacheConfig.isInvalidateOnChange());

        // Test that the testMapConfig2's mapStoreConfig implementation
        MapConfig testMapConfig2 = config.getMapConfig("testMap2");
        assertNotNull(testMapConfig2.getMapStoreConfig().getImplementation());
        assertEquals(dummyMapStore, testMapConfig2.getMapStoreConfig().getImplementation());
        assertEquals(MapStoreConfig.InitialLoadMode.LAZY, testMapConfig2.getMapStoreConfig().getInitialLoadMode());
        assertEquals("testWan", testMapConfig2.getWanReplicationRef().getName());
        assertEquals(1000, testMapConfig2.getMaxSizeConfig().getSize());
        assertEquals(MaxSizeConfig.MaxSizePolicy.PER_NODE, testMapConfig2.getMaxSizeConfig().getMaxSizePolicy());
        assertEquals(2, testMapConfig2.getEntryListenerConfigs().size());
        for (EntryListenerConfig listener : testMapConfig2.getEntryListenerConfigs()) {
            if (listener.getClassName() != null) {
                assertNull(listener.getImplementation());
                assertTrue(listener.isIncludeValue());
                assertFalse(listener.isLocal());
            } else {
                assertNotNull(listener.getImplementation());
                assertEquals(entryListener, listener.getImplementation());
                assertTrue(listener.isLocal());
                assertTrue(listener.isIncludeValue());
            }
        }

        MapConfig simpleMapConfig = config.getMapConfig("simpleMap");
        assertNotNull(simpleMapConfig);
        assertEquals("simpleMap", simpleMapConfig.getName());
        assertEquals(3, simpleMapConfig.getBackupCount());
        assertEquals(1, simpleMapConfig.getAsyncBackupCount());
        assertEquals(EvictionPolicy.LRU, simpleMapConfig.getEvictionPolicy());
        assertEquals(10, simpleMapConfig.getMaxSizeConfig().getSize());
        assertEquals(50, simpleMapConfig.getEvictionPercentage());
        assertEquals(1, simpleMapConfig.getTimeToLiveSeconds());
        assertEquals("LATEST_UPDATE", simpleMapConfig.getMergePolicy());
        // Test that the simpleMapConfig does NOT have a mapStoreConfig
        assertNull(simpleMapConfig.getMapStoreConfig());
        // Test that the simpleMapConfig does NOT have a nearCacheConfig
        assertNull(simpleMapConfig.getNearCacheConfig());

        MapConfig testMapConfig3 = config.getMapConfig("testMap3");
        assertEquals("com.hazelcast.spring.DummyStoreFactory", testMapConfig3.getMapStoreConfig().getFactoryClassName());
        assertFalse(testMapConfig3.getMapStoreConfig().getProperties().isEmpty());
        assertEquals(testMapConfig3.getMapStoreConfig().getProperty("dummy.property"), "value");

        MapConfig testMapConfig4 = config.getMapConfig("testMap4");
        assertEquals(dummyMapStoreFactory, testMapConfig4.getMapStoreConfig().getFactoryImplementation());

        MapConfig mapWithOptimizedQueriesConfig = config.getMapConfig("mapWithOptimizedQueries");
        assertTrue(mapWithOptimizedQueriesConfig.isOptimizeQueries());

        MapConfig mapWithNotOptimizedQueriesConfig = config.getMapConfig("mapWithNotOptimizedQueries");
        assertFalse(mapWithNotOptimizedQueriesConfig.isOptimizeQueries());

        MapConfig mapWithDefaultOptimizedQueriesConfig = config.getMapConfig("mapWithDefaultOptimizedQueries");
        assertFalse(mapWithDefaultOptimizedQueriesConfig.isOptimizeQueries());
    }

    @Test
    public void testQueueConfig() {
        QueueConfig testQConfig = config.getQueueConfig("testQ");
        assertNotNull(testQConfig);
        assertEquals("testQ", testQConfig.getName());
        assertEquals(1000, testQConfig.getMaxSize());
        QueueConfig qConfig = config.getQueueConfig("q");
        assertNotNull(qConfig);
        assertEquals("q", qConfig.getName());
        assertEquals(2500, qConfig.getMaxSize());
        assertEquals(1, testQConfig.getItemListenerConfigs().size());
        ItemListenerConfig listenerConfig = testQConfig.getItemListenerConfigs().get(0);
        assertEquals("com.hazelcast.spring.DummyItemListener", listenerConfig.getClassName());
        assertTrue(listenerConfig.isIncludeValue());
    }

    @Test
    public void testMultimapConfig() {
        MultiMapConfig testMultiMapConfig = config.getMultiMapConfig("testMultimap");
        assertEquals(MultiMapConfig.ValueCollectionType.LIST, testMultiMapConfig.getValueCollectionType());
        assertEquals(2, testMultiMapConfig.getEntryListenerConfigs().size());
        for (EntryListenerConfig listener : testMultiMapConfig.getEntryListenerConfigs()) {
            if (listener.getClassName() != null) {
                assertNull(listener.getImplementation());
                assertTrue(listener.isIncludeValue());
                assertFalse(listener.isLocal());
            } else {
                assertNotNull(listener.getImplementation());
                assertEquals(entryListener, listener.getImplementation());
                assertTrue(listener.isLocal());
                assertTrue(listener.isIncludeValue());
            }
        }
    }

    @Test
    public void testTopicConfig() {
        TopicConfig testTopicConfig = config.getTopicConfig("testTopic");
        assertNotNull(testTopicConfig);
        assertEquals("testTopic", testTopicConfig.getName());
        assertEquals(1, testTopicConfig.getMessageListenerConfigs().size());
        assertEquals(true, testTopicConfig.isGlobalOrderingEnabled());
        assertEquals(false, testTopicConfig.isStatisticsEnabled());
        ListenerConfig listenerConfig = testTopicConfig.getMessageListenerConfigs().get(0);
        assertEquals("com.hazelcast.spring.DummyMessageListener", listenerConfig.getClassName());
    }

    @Test
    public void testServiceConfig() {
        ServiceConfig serviceConfig = config.getServicesConfig().getServiceConfig("my-service");
        assertEquals("com.hazelcast.spring.MyService", serviceConfig.getClassName());
        assertEquals("prop1-value", serviceConfig.getProperties().getProperty("prop1"));
        assertEquals("prop2-value", serviceConfig.getProperties().getProperty("prop2"));
        MyServiceConfig configObject = (MyServiceConfig) serviceConfig.getConfigObject();
        assertNotNull(configObject);
        assertEquals("prop1", configObject.stringProp);
        assertEquals(123, configObject.intProp);
        assertTrue(configObject.boolProp);
    }

    @Test
    public void testGroupConfig() {
        GroupConfig groupConfig = config.getGroupConfig();
        assertNotNull(groupConfig);
        assertEquals("spring-group", groupConfig.getName());
        assertEquals("spring-group-pass", groupConfig.getPassword());
    }

    @Test
    public void testExecutorConfig() {
        ExecutorConfig testExecConfig = config.getExecutorConfig("testExec");
        assertNotNull(testExecConfig);
        assertEquals("testExec", testExecConfig.getName());
        assertEquals(2, testExecConfig.getPoolSize());
        assertEquals(100, testExecConfig.getQueueCapacity());
        ExecutorConfig testExec2Config = config.getExecutorConfig("testExec2");
        assertNotNull(testExec2Config);
        assertEquals("testExec2", testExec2Config.getName());
        assertEquals(5, testExec2Config.getPoolSize());
        assertEquals(300, testExec2Config.getQueueCapacity());
    }

    @Test
    public void testNetworkConfig() {
        NetworkConfig networkConfig = config.getNetworkConfig();
        assertNotNull(networkConfig);
        assertEquals(5700, networkConfig.getPort());
        assertFalse(networkConfig.isPortAutoIncrement());
        final Collection<String> allowedPorts = networkConfig.getOutboundPortDefinitions();
        assertEquals(2, allowedPorts.size());
        Iterator portIter = allowedPorts.iterator();
        assertEquals("35000-35100", portIter.next());
        assertEquals("36000,36100", portIter.next());
        assertFalse(networkConfig.getJoin().getMulticastConfig().isEnabled());
        assertEquals(networkConfig.getJoin().getMulticastConfig().getMulticastTimeoutSeconds(), 8);
        assertEquals(networkConfig.getJoin().getMulticastConfig().getMulticastTimeToLive(), 16);
        assertFalse(networkConfig.getInterfaces().isEnabled());
        assertEquals(1, networkConfig.getInterfaces().getInterfaces().size());
        assertEquals("10.10.1.*", networkConfig.getInterfaces().getInterfaces().iterator().next());
        TcpIpConfig tcp = networkConfig.getJoin().getTcpIpConfig();
        assertNotNull(tcp);
        assertTrue(tcp.isEnabled());
        assertTrue(networkConfig.getSymmetricEncryptionConfig().isEnabled());
        final List<String> members = tcp.getMembers();
        assertEquals(members.toString(), 2, members.size());
        assertEquals("127.0.0.1:5700", members.get(0));
        assertEquals("127.0.0.1:5701", members.get(1));
        assertEquals("127.0.0.1:5700", tcp.getRequiredMember());
        AwsConfig aws = networkConfig.getJoin().getAwsConfig();
        assertFalse(aws.isEnabled());
        assertEquals("sample-access-key", aws.getAccessKey());
        assertEquals("sample-secret-key", aws.getSecretKey());
        assertEquals("sample-region", aws.getRegion());
        assertEquals("sample-group", aws.getSecurityGroupName());
        assertEquals("sample-tag-key", aws.getTagKey());
        assertEquals("sample-tag-value", aws.getTagValue());

        assertTrue("reuse-address", networkConfig.isReuseAddress());
    }

//    @Test
//    public void testSemaphoreConfig() {
//        SemaphoreConfig testSemaphoreConfig = config.getSemaphoreConfig("testSemaphore");
//        assertNotNull(testSemaphoreConfig);
//        assertEquals(testSemaphoreConfig.getInitialPermits(), 0);
//        assertEquals(testSemaphoreConfig.isFactoryEnabled(), false);
//        assertNull(testSemaphoreConfig.getFactoryClassName());
//    }

    @Test
    public void testProperties() {
        final Properties properties = config.getProperties();
        assertNotNull(properties);
        assertEquals("5", properties.get(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS));
        assertEquals("5", properties.get(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS));
        final Config config2 = instance.getConfig();
        final Properties properties2 = config2.getProperties();
        assertNotNull(properties2);
        assertEquals("5", properties2.get(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS));
        assertEquals("5", properties2.get(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS));
    }

    @Test
    public void testInstance() {
        assertNotNull(instance);
        final Set<Member> members = instance.getCluster().getMembers();
        assertEquals(1, members.size());
        final Member member = members.iterator().next();
        final InetSocketAddress inetSocketAddress = member.getInetSocketAddress();
        assertEquals(5700, inetSocketAddress.getPort());
        assertEquals("test-instance", config.getInstanceName());
        assertEquals("HAZELCAST_ENTERPRISE_LICENSE_KEY", config.getLicenseKey());
    }

    @Test
    public void testHazelcastInstances() {
        assertNotNull(map1);
        assertNotNull(map2);
        assertNotNull(multiMap);
        assertNotNull(queue);
        assertNotNull(topic);
        assertNotNull(set);
        assertNotNull(list);
        assertNotNull(executorService);
        assertNotNull(idGenerator);
        assertNotNull(atomicLong);
        assertNotNull(atomicReference);
        assertNotNull(countDownLatch);
        assertNotNull(semaphore);
        assertNotNull(lock);
        assertEquals("map1", map1.getName());
        assertEquals("map2", map2.getName());
        assertEquals("testMultimap", multiMap.getName());
        assertEquals("testQ", queue.getName());
        assertEquals("testTopic", topic.getName());
        assertEquals("set", set.getName());
        assertEquals("list", list.getName());
        assertEquals("idGenerator", idGenerator.getName());
        assertEquals("atomicLong", atomicLong.getName());
        assertEquals("atomicReference", atomicReference.getName());
        assertEquals("countDownLatch", countDownLatch.getName());
        assertEquals("semaphore", semaphore.getName());
    }

    @Test
    public void testWanReplicationConfig() {
        WanReplicationConfig wcfg = config.getWanReplicationConfig("testWan");
        assertNotNull(wcfg);
        assertEquals(2, wcfg.getTargetClusterConfigs().size());
        WanTargetClusterConfig targetCfg = wcfg.getTargetClusterConfigs().get(0);
        assertNotNull(targetCfg);
        assertEquals("tokyo", targetCfg.getGroupName());
        assertEquals("tokyo-pass", targetCfg.getGroupPassword());
        assertEquals("com.hazelcast.wan.impl.WanNoDelayReplication", targetCfg.getReplicationImpl());
        assertEquals(2, targetCfg.getEndpoints().size());
        assertEquals("10.2.1.1:5701", targetCfg.getEndpoints().get(0));
        assertEquals("10.2.1.2:5701", targetCfg.getEndpoints().get(1));
        assertEquals(wanReplication, wcfg.getTargetClusterConfigs().get(1).getReplicationImplObject());
    }

    @Test
    public void testConfigListeners() {
        assertNotNull(membershipListener);
        List<ListenerConfig> list = config.getListenerConfigs();
        assertEquals(2, list.size());
        for (ListenerConfig lc : list) {
            if (lc.getClassName() != null) {
                assertNull(lc.getImplementation());
                assertEquals(DummyMembershipListener.class.getName(), lc.getClassName());
            } else {
                assertNotNull(lc.getImplementation());
                assertEquals(membershipListener, lc.getImplementation());
            }
        }
    }

    @Test
    public void testPartitionGroupConfig() {
        PartitionGroupConfig pgc = config.getPartitionGroupConfig();
        assertTrue(pgc.isEnabled());
        assertEquals(PartitionGroupConfig.MemberGroupType.CUSTOM, pgc.getGroupType());
        assertEquals(2, pgc.getMemberGroupConfigs().size());
        Iterator<MemberGroupConfig> iter = pgc.getMemberGroupConfigs().iterator();
        while (iter.hasNext()) {
            MemberGroupConfig mgc = iter.next();
            assertEquals(2, mgc.getInterfaces().size());
        }
    }

    @Test
    public void testSSLConfig() {
        SSLConfig sslConfig = config.getNetworkConfig().getSSLConfig();
        assertNotNull(sslConfig);
        assertFalse(sslConfig.isEnabled());
        assertEquals(DummySSLContextFactory.class.getName(), sslConfig.getFactoryClassName());
        assertEquals(sslContextFactory, sslConfig.getFactoryImplementation());
    }

    @Test
    public void testSocketInterceptorConfig() {
        SocketInterceptorConfig socketInterceptorConfig = config.getNetworkConfig().getSocketInterceptorConfig();
        assertNotNull(socketInterceptorConfig);
        assertFalse(socketInterceptorConfig.isEnabled());
        assertEquals(DummySocketInterceptor.class.getName(), socketInterceptorConfig.getClassName());
        assertEquals(socketInterceptor, socketInterceptorConfig.getImplementation());
    }

    @Test
    public void testManagementCenterConfig() {
        ManagementCenterConfig managementCenterConfig = config.getManagementCenterConfig();
        assertNotNull(managementCenterConfig);
        assertTrue(managementCenterConfig.isEnabled());
        assertEquals("myserver:80", managementCenterConfig.getUrl());
        assertEquals(4, managementCenterConfig.getUpdateInterval());
    }

    @Test
    public void testMemberAttributesConfig() {
        MemberAttributeConfig memberAttributeConfig = config.getMemberAttributeConfig();
        assertNotNull(memberAttributeConfig);
        assertEquals("spring-group", memberAttributeConfig.getStringAttribute("cluster.group.name"));
        assertEquals(new Integer(5700), memberAttributeConfig.getIntAttribute("cluster.port.int"));
        assertEquals(new Long(5700), memberAttributeConfig.getLongAttribute("cluster.port.long"));
        assertEquals(new Short("5700"), memberAttributeConfig.getShortAttribute("cluster.port.short"));
        assertEquals(new Byte("111"), memberAttributeConfig.getByteAttribute("attribute.byte"));
        assertTrue(memberAttributeConfig.getBooleanAttribute("attribute.boolean"));
        assertEquals(0.0d, memberAttributeConfig.getDoubleAttribute("attribute.double"), 0.0001d);
        assertEquals(1234.5678, memberAttributeConfig.getFloatAttribute("attribute.float"), 0.0001);
    }

    @Test
    public void testSerializationConfig() {
        SerializationConfig serializationConfig = config.getSerializationConfig();
        assertEquals(ByteOrder.BIG_ENDIAN, serializationConfig.getByteOrder());
        assertEquals(false, serializationConfig.isCheckClassDefErrors());
        assertEquals(13, serializationConfig.getPortableVersion());

        Map<Integer, String> dataSerializableFactoryClasses = serializationConfig
                .getDataSerializableFactoryClasses();
        assertFalse(dataSerializableFactoryClasses.isEmpty());
        assertEquals(DummyDataSerializableFactory.class.getName(), dataSerializableFactoryClasses.get(1));

        Map<Integer, DataSerializableFactory> dataSerializableFactories = serializationConfig
                .getDataSerializableFactories();
        assertFalse(dataSerializableFactories.isEmpty());
        assertEquals(DummyDataSerializableFactory.class, dataSerializableFactories.get(2).getClass());

        Map<Integer, String> portableFactoryClasses = serializationConfig.getPortableFactoryClasses();
        assertFalse(portableFactoryClasses.isEmpty());
        assertEquals(DummyPortableFactory.class.getName(), portableFactoryClasses.get(1));

        Map<Integer, PortableFactory> portableFactories = serializationConfig.getPortableFactories();
        assertFalse(portableFactories.isEmpty());
        assertEquals(DummyPortableFactory.class, portableFactories.get(2).getClass());

        Collection<SerializerConfig> serializerConfigs = serializationConfig.getSerializerConfigs();
        assertFalse(serializerConfigs.isEmpty());

        GlobalSerializerConfig globalSerializerConfig = serializationConfig.getGlobalSerializerConfig();
        assertNotNull(globalSerializerConfig);
        assertEquals(dummySerializer, globalSerializerConfig.getImplementation());
    }
}
