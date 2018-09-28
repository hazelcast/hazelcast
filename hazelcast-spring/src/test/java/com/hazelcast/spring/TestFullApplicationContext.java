/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.AtomicLongConfig;
import com.hazelcast.config.AtomicReferenceConfig;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.AzureConfig;
import com.hazelcast.config.CRDTReplicationConfig;
import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.ClassFilter;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.CountDownLatchConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EurekaConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.GcpConfig;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.IcmpFailureDetectorConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.config.KubernetesConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.LockConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MemberAddressProviderConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.MemberGroupConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionConfig.PermissionType;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanPublisherState;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.WanSyncConfig;
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
import com.hazelcast.core.QueueStore;
import com.hazelcast.core.QueueStoreFactory;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.core.RingbufferStore;
import com.hazelcast.core.RingbufferStoreFactory;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.nio.ssl.SSLContextFactory;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.quorum.impl.ProbabilisticQuorumFunction;
import com.hazelcast.quorum.impl.RecentlyActiveQuorumFunction;
import com.hazelcast.spring.serialization.DummyDataSerializableFactory;
import com.hazelcast.spring.serialization.DummyPortableFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.TopicOverloadPolicy;
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
import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.hazelcast.config.HotRestartClusterDataRecoveryPolicy.PARTIAL_RECOVERY_MOST_COMPLETE;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static com.hazelcast.util.CollectionUtil.isNotEmpty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"fullConfig-applicationContext-hazelcast.xml"})
@Category(QuickTest.class)
@SuppressWarnings("unused")
public class TestFullApplicationContext extends HazelcastTestSupport {

    private Config config;

    @Resource(name = "instance")
    private HazelcastInstance instance;

    @Resource(name = "map1")
    private IMap<Object, Object> map1;

    @Resource(name = "map2")
    private IMap<Object, Object> map2;

    @Resource(name = "multiMap")
    private MultiMap multiMap;

    @Resource(name = "replicatedMap")
    private ReplicatedMap replicatedMap;

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

    @Resource(name = "flakeIdGenerator")
    private FlakeIdGenerator flakeIdGenerator;

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

    @Resource(name = "dummyQueueStore")
    private QueueStore dummyQueueStore;
    @Autowired
    private QueueStoreFactory dummyQueueStoreFactory;

    @Resource(name = "dummyRingbufferStore")
    private RingbufferStore dummyRingbufferStore;
    @Autowired
    private RingbufferStoreFactory dummyRingbufferStoreFactory;

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

    @Resource(name = "pnCounter")
    private PNCounter pnCounter;

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
    public void testCacheConfig() {
        assertNotNull(config);
        assertEquals(1, config.getCacheConfigs().size());
        CacheSimpleConfig cacheConfig = config.getCacheConfig("testCache");
        assertEquals("testCache", cacheConfig.getName());
        assertTrue(cacheConfig.isDisablePerEntryInvalidationEvents());
        assertTrue(cacheConfig.getHotRestartConfig().isEnabled());
        assertTrue(cacheConfig.getHotRestartConfig().isFsync());

        WanReplicationRef wanRef = cacheConfig.getWanReplicationRef();
        assertEquals("testWan", wanRef.getName());
        assertEquals("PUT_IF_ABSENT", wanRef.getMergePolicy());
        assertEquals(1, wanRef.getFilters().size());
        assertEquals("com.example.SampleFilter", wanRef.getFilters().get(0));
    }

    @Test
    public void testMapConfig() {
        assertNotNull(config);
        assertEquals(27, config.getMapConfigs().size());

        MapConfig testMapConfig = config.getMapConfig("testMap");
        assertNotNull(testMapConfig);
        assertEquals("testMap", testMapConfig.getName());
        assertEquals(2, testMapConfig.getBackupCount());
        assertEquals(EvictionPolicy.NONE, testMapConfig.getEvictionPolicy());
        assertEquals(Integer.MAX_VALUE, testMapConfig.getMaxSizeConfig().getSize());
        assertEquals(30, testMapConfig.getEvictionPercentage());
        assertEquals(0, testMapConfig.getTimeToLiveSeconds());
        assertTrue(testMapConfig.getHotRestartConfig().isEnabled());
        assertTrue(testMapConfig.getHotRestartConfig().isFsync());
        assertEquals(1000, testMapConfig.getMinEvictionCheckMillis());
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
        assertEquals(2, testMapConfig.getMapAttributeConfigs().size());
        for (MapAttributeConfig attribute : testMapConfig.getMapAttributeConfigs()) {
            if ("power".equals(attribute.getName())) {
                assertEquals("com.car.PowerExtractor", attribute.getExtractor());
            } else if ("weight".equals(attribute.getName())) {
                assertEquals("com.car.WeightExtractor", attribute.getExtractor());
            } else {
                fail("unknown attribute!");
            }
        }
        assertEquals("my-quorum", testMapConfig.getQuorumName());
        MergePolicyConfig mergePolicyConfig = testMapConfig.getMergePolicyConfig();
        assertNotNull(mergePolicyConfig);
        assertEquals("PassThroughMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(2342, mergePolicyConfig.getBatchSize());

        // test that the testMapConfig has a mapStoreConfig and it is correct
        MapStoreConfig testMapStoreConfig = testMapConfig.getMapStoreConfig();
        assertNotNull(testMapStoreConfig);
        assertEquals("com.hazelcast.spring.DummyStore", testMapStoreConfig.getClassName());
        assertTrue(testMapStoreConfig.isEnabled());
        assertEquals(0, testMapStoreConfig.getWriteDelaySeconds());
        assertEquals(10, testMapStoreConfig.getWriteBatchSize());
        assertTrue(testMapStoreConfig.isWriteCoalescing());
        assertEquals(MapStoreConfig.InitialLoadMode.EAGER, testMapStoreConfig.getInitialLoadMode());

        // test that the testMapConfig has a nearCacheConfig and it is correct
        NearCacheConfig testNearCacheConfig = testMapConfig.getNearCacheConfig();
        assertNotNull(testNearCacheConfig);
        assertEquals(0, testNearCacheConfig.getTimeToLiveSeconds());
        assertEquals(60, testNearCacheConfig.getMaxIdleSeconds());
        assertEquals(EvictionPolicy.LRU, testNearCacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals(5000, testNearCacheConfig.getEvictionConfig().getSize());
        assertTrue(testNearCacheConfig.isInvalidateOnChange());
        assertFalse(testNearCacheConfig.isSerializeKeys());

        // test that the testMapConfig2's mapStoreConfig implementation
        MapConfig testMapConfig2 = config.getMapConfig("testMap2");
        assertNotNull(testMapConfig2.getMapStoreConfig().getImplementation());
        assertEquals(dummyMapStore, testMapConfig2.getMapStoreConfig().getImplementation());
        assertEquals(MapStoreConfig.InitialLoadMode.LAZY, testMapConfig2.getMapStoreConfig().getInitialLoadMode());

        // test testMapConfig2's WanReplicationConfig
        WanReplicationRef wanReplicationRef = testMapConfig2.getWanReplicationRef();
        assertEquals("testWan", wanReplicationRef.getName());
        assertEquals("PUT_IF_ABSENT", wanReplicationRef.getMergePolicy());
        assertTrue(wanReplicationRef.isRepublishingEnabled());

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

        // test that the simpleMapConfig does NOT have a nearCacheConfig
        assertNull(simpleMapConfig.getNearCacheConfig());

        MapConfig testMapConfig3 = config.getMapConfig("testMap3");
        assertEquals("com.hazelcast.spring.DummyStoreFactory", testMapConfig3.getMapStoreConfig().getFactoryClassName());
        assertFalse(testMapConfig3.getMapStoreConfig().getProperties().isEmpty());
        assertEquals(testMapConfig3.getMapStoreConfig().getProperty("dummy.property"), "value");

        MapConfig testMapConfig4 = config.getMapConfig("testMap4");
        assertEquals(dummyMapStoreFactory, testMapConfig4.getMapStoreConfig().getFactoryImplementation());

        MapConfig mapWithOptimizedQueriesConfig = config.getMapConfig("mapWithOptimizedQueries");
        assertEquals(CacheDeserializedValues.ALWAYS, mapWithOptimizedQueriesConfig.getCacheDeserializedValues());

        MapConfig mapWithValueCachingSetToNever = config.getMapConfig("mapWithValueCachingSetToNever");
        assertEquals(CacheDeserializedValues.NEVER, mapWithValueCachingSetToNever.getCacheDeserializedValues());

        MapConfig mapWithValueCachingSetToAlways = config.getMapConfig("mapWithValueCachingSetToAlways");
        assertEquals(CacheDeserializedValues.ALWAYS, mapWithValueCachingSetToAlways.getCacheDeserializedValues());

        MapConfig mapWithNotOptimizedQueriesConfig = config.getMapConfig("mapWithNotOptimizedQueries");
        assertEquals(CacheDeserializedValues.INDEX_ONLY, mapWithNotOptimizedQueriesConfig.getCacheDeserializedValues());

        MapConfig mapWithDefaultOptimizedQueriesConfig = config.getMapConfig("mapWithDefaultOptimizedQueries");
        assertEquals(CacheDeserializedValues.INDEX_ONLY, mapWithDefaultOptimizedQueriesConfig.getCacheDeserializedValues());

        MapConfig testMapWithPartitionLostListenerConfig = config.getMapConfig("mapWithPartitionLostListener");
        List<MapPartitionLostListenerConfig> partitionLostListenerConfigs
                = testMapWithPartitionLostListenerConfig.getPartitionLostListenerConfigs();
        assertEquals(1, partitionLostListenerConfigs.size());
        assertEquals("DummyMapPartitionLostListenerImpl", partitionLostListenerConfigs.get(0).getClassName());

        MapConfig testMapWithPartitionStrategyConfig = config.getMapConfig("mapWithPartitionStrategy");
        assertEquals("com.hazelcast.spring.DummyPartitionStrategy",
                testMapWithPartitionStrategyConfig.getPartitioningStrategyConfig().getPartitioningStrategyClass());
    }

    @Test
    public void testMapNoWanMergePolicy() {
        MapConfig testMapConfig2 = config.getMapConfig("testMap2");


        // test testMapConfig2's WanReplicationConfig
        WanReplicationRef wanReplicationRef = testMapConfig2.getWanReplicationRef();
        assertEquals("testWan", wanReplicationRef.getName());
        assertEquals("PUT_IF_ABSENT", wanReplicationRef.getMergePolicy());
    }

    @Test
    public void testMemberFlakeIdGeneratorConfig() {
        FlakeIdGeneratorConfig c = instance.getConfig().findFlakeIdGeneratorConfig("flakeIdGenerator");
        assertEquals(3, c.getPrefetchCount());
        assertEquals(10L, c.getPrefetchValidityMillis());
        assertEquals(20L, c.getIdOffset());
        assertEquals(30L, c.getNodeIdOffset());
        assertEquals("flakeIdGenerator*", c.getName());
        assertFalse(c.isStatisticsEnabled());
    }

    @Test
    public void testQueueConfig() {
        QueueConfig testQConfig = config.getQueueConfig("testQ");
        assertNotNull(testQConfig);
        assertEquals("testQ", testQConfig.getName());
        assertEquals(1000, testQConfig.getMaxSize());
        assertEquals(1, testQConfig.getItemListenerConfigs().size());
        assertTrue(testQConfig.isStatisticsEnabled());
        ItemListenerConfig listenerConfig = testQConfig.getItemListenerConfigs().get(0);
        assertEquals("com.hazelcast.spring.DummyItemListener", listenerConfig.getClassName());
        assertTrue(listenerConfig.isIncludeValue());

        QueueConfig qConfig = config.getQueueConfig("queueWithSplitBrainConfig");
        assertNotNull(qConfig);
        assertEquals("queueWithSplitBrainConfig", qConfig.getName());
        assertEquals(2500, qConfig.getMaxSize());
        assertFalse(qConfig.isStatisticsEnabled());
        assertEquals(100, qConfig.getEmptyQueueTtl());
        assertEquals("my-quorum", qConfig.getQuorumName());
        MergePolicyConfig mergePolicyConfig = qConfig.getMergePolicyConfig();
        assertEquals("DiscardMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(2342, mergePolicyConfig.getBatchSize());

        QueueConfig queueWithStore1 = config.getQueueConfig("queueWithStore1");
        assertNotNull(queueWithStore1);
        QueueStoreConfig storeConfig1 = queueWithStore1.getQueueStoreConfig();
        assertNotNull(storeConfig1);
        assertEquals(DummyQueueStore.class.getName(), storeConfig1.getClassName());

        QueueConfig queueWithStore2 = config.getQueueConfig("queueWithStore2");
        assertNotNull(queueWithStore2);
        QueueStoreConfig storeConfig2 = queueWithStore2.getQueueStoreConfig();
        assertNotNull(storeConfig2);
        assertEquals(DummyQueueStoreFactory.class.getName(), storeConfig2.getFactoryClassName());

        QueueConfig queueWithStore3 = config.getQueueConfig("queueWithStore3");
        assertNotNull(queueWithStore3);
        QueueStoreConfig storeConfig3 = queueWithStore3.getQueueStoreConfig();
        assertNotNull(storeConfig3);
        assertEquals(dummyQueueStore, storeConfig3.getStoreImplementation());

        QueueConfig queueWithStore4 = config.getQueueConfig("queueWithStore4");
        assertNotNull(queueWithStore4);
        QueueStoreConfig storeConfig4 = queueWithStore4.getQueueStoreConfig();
        assertNotNull(storeConfig4);
        assertEquals(dummyQueueStoreFactory, storeConfig4.getFactoryImplementation());
    }

    @Test
    public void testLockConfig() {
        LockConfig lockConfig = config.getLockConfig("lock");
        assertNotNull(lockConfig);
        assertEquals("lock", lockConfig.getName());
        assertEquals("my-quorum", lockConfig.getQuorumName());
    }

    @Test
    public void testRingbufferConfig() {
        RingbufferConfig testRingbuffer = config.getRingbufferConfig("testRingbuffer");
        assertNotNull(testRingbuffer);
        assertEquals("testRingbuffer", testRingbuffer.getName());
        assertEquals(InMemoryFormat.OBJECT, testRingbuffer.getInMemoryFormat());
        assertEquals(100, testRingbuffer.getCapacity());
        assertEquals(1, testRingbuffer.getBackupCount());
        assertEquals(1, testRingbuffer.getAsyncBackupCount());
        assertEquals(20, testRingbuffer.getTimeToLiveSeconds());
        RingbufferStoreConfig store1 = testRingbuffer.getRingbufferStoreConfig();
        assertNotNull(store1);
        assertEquals(DummyRingbufferStore.class.getName(), store1.getClassName());
        MergePolicyConfig mergePolicyConfig = testRingbuffer.getMergePolicyConfig();
        assertNotNull(mergePolicyConfig);
        assertEquals("PassThroughMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(2342, mergePolicyConfig.getBatchSize());

        RingbufferConfig testRingbuffer2 = config.getRingbufferConfig("testRingbuffer2");
        assertNotNull(testRingbuffer2);
        RingbufferStoreConfig store2 = testRingbuffer2.getRingbufferStoreConfig();
        assertNotNull(store2);
        assertEquals(DummyRingbufferStoreFactory.class.getName(), store2.getFactoryClassName());
        assertFalse(store2.getProperties().isEmpty());
        assertEquals("value", store2.getProperty("dummy.property"));
        assertEquals("value2", store2.getProperty("dummy.property.2"));

        RingbufferConfig testRingbuffer3 = config.getRingbufferConfig("testRingbuffer3");
        assertNotNull(testRingbuffer3);
        RingbufferStoreConfig store3 = testRingbuffer3.getRingbufferStoreConfig();
        assertNotNull(store3);
        assertEquals(dummyRingbufferStore, store3.getStoreImplementation());

        RingbufferConfig testRingbuffer4 = config.getRingbufferConfig("testRingbuffer4");
        assertNotNull(testRingbuffer4);
        RingbufferStoreConfig store4 = testRingbuffer4.getRingbufferStoreConfig();
        assertNotNull(store4);
        assertEquals(dummyRingbufferStoreFactory, store4.getFactoryImplementation());
    }

    @Test
    public void testPNCounterConfig() {
        PNCounterConfig testPNCounter = config.getPNCounterConfig("testPNCounter");
        assertNotNull(testPNCounter);
        assertEquals("testPNCounter", testPNCounter.getName());
        assertEquals(100, testPNCounter.getReplicaCount());
        assertEquals("my-quorum", testPNCounter.getQuorumName());
        assertFalse(testPNCounter.isStatisticsEnabled());
    }

    @Test
    public void testSecurity() {
        final Set<PermissionConfig> clientPermissionConfigs = config.getSecurityConfig().getClientPermissionConfigs();
        assertFalse(config.getSecurityConfig().getClientBlockUnmappedActions());
        assertTrue(isNotEmpty(clientPermissionConfigs));
        assertEquals(1, clientPermissionConfigs.size());
        final PermissionConfig pnCounterPermission = new PermissionConfig(PermissionType.PN_COUNTER, "pnCounterPermission", "*")
                .addAction("create")
                .setEndpoints(Collections.<String>emptySet());

        assertContains(clientPermissionConfigs, pnCounterPermission);
    }

    @Test
    public void testAtomicLongConfig() {
        AtomicLongConfig testAtomicLong = config.getAtomicLongConfig("testAtomicLong");
        assertNotNull(testAtomicLong);
        assertEquals("testAtomicLong", testAtomicLong.getName());

        MergePolicyConfig mergePolicyConfig = testAtomicLong.getMergePolicyConfig();
        assertEquals("DiscardMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(2342, mergePolicyConfig.getBatchSize());
    }

    @Test
    public void testAtomicReferenceConfig() {
        AtomicReferenceConfig testAtomicReference = config.getAtomicReferenceConfig("testAtomicReference");
        assertNotNull(testAtomicReference);
        assertEquals("testAtomicReference", testAtomicReference.getName());

        MergePolicyConfig mergePolicyConfig = testAtomicReference.getMergePolicyConfig();
        assertEquals("PassThroughMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(4223, mergePolicyConfig.getBatchSize());
    }

    @Test
    public void testCountDownLatchConfig() {
        CountDownLatchConfig testCountDownLatch = config.getCountDownLatchConfig("testCountDownLatch");
        assertNotNull(testCountDownLatch);
        assertEquals("testCountDownLatch", testCountDownLatch.getName());
        assertEquals("my-quorum", testCountDownLatch.getQuorumName());
    }

    @Test
    public void testSemaphoreConfig() {
        SemaphoreConfig testSemaphore = config.getSemaphoreConfig("testSemaphore");
        assertNotNull(testSemaphore);
        assertEquals("testSemaphore", testSemaphore.getName());
        assertEquals(1, testSemaphore.getBackupCount());
        assertEquals(1, testSemaphore.getAsyncBackupCount());
        assertEquals(10, testSemaphore.getInitialPermits());
    }

    @Test
    public void testReliableTopicConfig() {
        ReliableTopicConfig testReliableTopic = config.getReliableTopicConfig("testReliableTopic");
        assertNotNull(testReliableTopic);
        assertEquals("testReliableTopic", testReliableTopic.getName());
        assertEquals(1, testReliableTopic.getMessageListenerConfigs().size());
        assertFalse(testReliableTopic.isStatisticsEnabled());
        ListenerConfig listenerConfig = testReliableTopic.getMessageListenerConfigs().get(0);
        assertEquals("com.hazelcast.spring.DummyMessageListener", listenerConfig.getClassName());
        assertEquals(10, testReliableTopic.getReadBatchSize());
        assertEquals(TopicOverloadPolicy.BLOCK, testReliableTopic.getTopicOverloadPolicy());
    }

    @Test
    public void testMultimapConfig() {
        MultiMapConfig testMultiMapConfig = config.getMultiMapConfig("testMultimap");
        assertEquals(MultiMapConfig.ValueCollectionType.LIST, testMultiMapConfig.getValueCollectionType());
        assertEquals(2, testMultiMapConfig.getEntryListenerConfigs().size());
        assertFalse(testMultiMapConfig.isBinary());
        assertFalse(testMultiMapConfig.isStatisticsEnabled());
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
        MergePolicyConfig mergePolicyConfig = testMultiMapConfig.getMergePolicyConfig();
        assertEquals("PassThroughMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(1234, mergePolicyConfig.getBatchSize());
    }

    @Test
    public void testListConfig() {
        ListConfig testListConfig = config.getListConfig("testList");
        assertNotNull(testListConfig);
        assertEquals("testList", testListConfig.getName());
        assertEquals(9999, testListConfig.getMaxSize());
        assertEquals(1, testListConfig.getBackupCount());
        assertEquals(1, testListConfig.getAsyncBackupCount());
        assertFalse(testListConfig.isStatisticsEnabled());

        MergePolicyConfig mergePolicyConfig = testListConfig.getMergePolicyConfig();
        assertEquals("DiscardMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(2342, mergePolicyConfig.getBatchSize());
    }

    @Test
    public void testSetConfig() {
        SetConfig testSetConfig = config.getSetConfig("testSet");
        assertNotNull(testSetConfig);
        assertEquals("testSet", testSetConfig.getName());
        assertEquals(7777, testSetConfig.getMaxSize());
        assertEquals(0, testSetConfig.getBackupCount());
        assertEquals(0, testSetConfig.getAsyncBackupCount());
        assertFalse(testSetConfig.isStatisticsEnabled());

        MergePolicyConfig mergePolicyConfig = testSetConfig.getMergePolicyConfig();
        assertEquals("DiscardMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(2342, mergePolicyConfig.getBatchSize());
    }

    @Test
    public void testTopicConfig() {
        TopicConfig testTopicConfig = config.getTopicConfig("testTopic");
        assertNotNull(testTopicConfig);
        assertEquals("testTopic", testTopicConfig.getName());
        assertEquals(1, testTopicConfig.getMessageListenerConfigs().size());
        assertTrue(testTopicConfig.isGlobalOrderingEnabled());
        assertFalse(testTopicConfig.isStatisticsEnabled());
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
        Object impl = serviceConfig.getImplementation();
        assertNotNull(impl);
        assertTrue("expected service of class com.hazelcast.spring.MyService but it is "
                + impl.getClass().getName(), impl instanceof MyService);
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
        assertTrue(testExecConfig.isStatisticsEnabled());
        ExecutorConfig testExec2Config = config.getExecutorConfig("testExec2");
        assertNotNull(testExec2Config);
        assertEquals("testExec2", testExec2Config.getName());
        assertEquals(5, testExec2Config.getPoolSize());
        assertEquals(300, testExec2Config.getQueueCapacity());
        assertFalse(testExec2Config.isStatisticsEnabled());
    }

    @Test
    public void testDurableExecutorConfig() {
        DurableExecutorConfig testExecConfig = config.getDurableExecutorConfig("durableExec");
        assertNotNull(testExecConfig);
        assertEquals("durableExec", testExecConfig.getName());
        assertEquals(10, testExecConfig.getPoolSize());
        assertEquals(5, testExecConfig.getDurability());
        assertEquals(200, testExecConfig.getCapacity());
    }

    @Test
    public void testScheduledExecutorConfig() {
        ScheduledExecutorConfig testExecConfig = config.getScheduledExecutorConfig("scheduledExec");
        assertNotNull(testExecConfig);
        assertEquals("scheduledExec", testExecConfig.getName());
        assertEquals(10, testExecConfig.getPoolSize());
        assertEquals(5, testExecConfig.getDurability());
        MergePolicyConfig mergePolicyConfig = testExecConfig.getMergePolicyConfig();
        assertNotNull(mergePolicyConfig);
        assertEquals("PassThroughMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(101, mergePolicyConfig.getBatchSize());
    }

    @Test
    public void testCardinalityEstimatorConfig() {
        CardinalityEstimatorConfig estimatorConfig = config.getCardinalityEstimatorConfig("estimator");
        assertNotNull(estimatorConfig);
        assertEquals("estimator", estimatorConfig.getName());
        assertEquals(4, estimatorConfig.getBackupCount());
        assertEquals("DiscardMergePolicy", estimatorConfig.getMergePolicyConfig().getPolicy());
        assertEquals(44, estimatorConfig.getMergePolicyConfig().getBatchSize());
    }

    @Test
    public void testNetworkConfig() {
        NetworkConfig networkConfig = config.getNetworkConfig();
        assertNotNull(networkConfig);
        assertEquals(5700, networkConfig.getPort());
        assertFalse(networkConfig.isPortAutoIncrement());

        Collection<String> allowedPorts = networkConfig.getOutboundPortDefinitions();
        assertEquals(2, allowedPorts.size());
        Iterator portIter = allowedPorts.iterator();
        assertEquals("35000-35100", portIter.next());
        assertEquals("36000,36100", portIter.next());
        assertFalse(networkConfig.getJoin().getMulticastConfig().isEnabled());
        assertEquals(networkConfig.getJoin().getMulticastConfig().getMulticastTimeoutSeconds(), 8);
        assertEquals(networkConfig.getJoin().getMulticastConfig().getMulticastTimeToLive(), 16);
        assertFalse(networkConfig.getJoin().getMulticastConfig().isLoopbackModeEnabled());
        Set<String> tis = networkConfig.getJoin().getMulticastConfig().getTrustedInterfaces();
        assertEquals(1, tis.size());
        assertEquals("10.10.10.*", tis.iterator().next());
        assertFalse(networkConfig.getInterfaces().isEnabled());
        assertEquals(1, networkConfig.getInterfaces().getInterfaces().size());
        assertEquals("10.10.1.*", networkConfig.getInterfaces().getInterfaces().iterator().next());
        TcpIpConfig tcp = networkConfig.getJoin().getTcpIpConfig();
        assertNotNull(tcp);
        assertTrue(tcp.isEnabled());
        SymmetricEncryptionConfig symmetricEncryptionConfig = networkConfig.getSymmetricEncryptionConfig();
        assertFalse(symmetricEncryptionConfig.isEnabled());
        assertEquals("PBEWithMD5AndDES", symmetricEncryptionConfig.getAlgorithm());
        assertEquals("thesalt", symmetricEncryptionConfig.getSalt());
        assertEquals("thepass", symmetricEncryptionConfig.getPassword());
        assertEquals(19, symmetricEncryptionConfig.getIterationCount());

        List<String> members = tcp.getMembers();
        assertEquals(members.toString(), 2, members.size());
        assertEquals("127.0.0.1:5700", members.get(0));
        assertEquals("127.0.0.1:5701", members.get(1));
        assertEquals("127.0.0.1:5700", tcp.getRequiredMember());
        assertAwsConfig(networkConfig.getJoin().getAwsConfig());
        assertGcpConfig(networkConfig.getJoin().getGcpConfig());
        assertAzureConfig(networkConfig.getJoin().getAzureConfig());
        assertKubernetesConfig(networkConfig.getJoin().getKubernetesConfig());
        assertEurekaConfig(networkConfig.getJoin().getEurekaConfig());

        assertTrue("reuse-address", networkConfig.isReuseAddress());

        assertDiscoveryConfig(networkConfig.getJoin().getDiscoveryConfig());

        MemberAddressProviderConfig memberAddressProviderConfig = networkConfig.getMemberAddressProviderConfig();
        assertFalse(memberAddressProviderConfig.isEnabled());
        assertEquals("com.hazelcast.spring.DummyMemberAddressProvider", memberAddressProviderConfig.getClassName());
        assertFalse(memberAddressProviderConfig.getProperties().isEmpty());
        assertEquals("value", memberAddressProviderConfig.getProperties().getProperty("dummy.property"));
        assertEquals("value2", memberAddressProviderConfig.getProperties().getProperty("dummy.property.2"));

        IcmpFailureDetectorConfig icmpFailureDetectorConfig = networkConfig.getIcmpFailureDetectorConfig();
        assertFalse(icmpFailureDetectorConfig.isEnabled());
        assertTrue(icmpFailureDetectorConfig.isParallelMode());
        assertTrue(icmpFailureDetectorConfig.isFailFastOnStartup());
        assertEquals(500, icmpFailureDetectorConfig.getTimeoutMilliseconds());
        assertEquals(1002, icmpFailureDetectorConfig.getIntervalMilliseconds());
        assertEquals(2, icmpFailureDetectorConfig.getMaxAttempts());
        assertEquals(1, icmpFailureDetectorConfig.getTtl());
    }

    private void assertAwsConfig(AwsConfig aws) {
        assertFalse(aws.isEnabled());
        assertEquals("sample-access-key", aws.getAccessKey());
        assertEquals("sample-secret-key", aws.getSecretKey());
        assertEquals("sample-region", aws.getRegion());
        assertEquals("sample-header", aws.getHostHeader());
        assertEquals("sample-group", aws.getSecurityGroupName());
        assertEquals("sample-tag-key", aws.getTagKey());
        assertEquals("sample-tag-value", aws.getTagValue());
        assertEquals("sample-role", aws.getIamRole());
    }

    private void assertGcpConfig(GcpConfig gcp) {
        assertFalse(gcp.isEnabled());
        assertEquals("us-east1-b,us-east1-c", gcp.getProperty("zones"));
    }

    private void assertAzureConfig(AzureConfig azure) {
        assertFalse(azure.isEnabled());
        assertEquals("CLIENT_ID", azure.getProperty("client-id"));
        assertEquals("CLIENT_SECRET", azure.getProperty("client-secret"));
        assertEquals("TENANT_ID", azure.getProperty("tenant-id"));
        assertEquals("SUB_ID", azure.getProperty("subscription-id"));
        assertEquals("HZLCAST001", azure.getProperty("cluster-id"));
        assertEquals("GROUP-NAME", azure.getProperty("group-name"));
    }

    private void assertKubernetesConfig(KubernetesConfig kubernetes) {
        assertFalse(kubernetes.isEnabled());
        assertEquals("MY-KUBERNETES-NAMESPACE", kubernetes.getProperty("namespace"));
        assertEquals("MY-SERVICE-NAME", kubernetes.getProperty("service-name"));
        assertEquals("MY-SERVICE-LABEL-NAME", kubernetes.getProperty("service-label-name"));
        assertEquals("MY-SERVICE-LABEL-VALUE", kubernetes.getProperty("service-label-value"));
    }

    private void assertEurekaConfig(EurekaConfig eureka) {
        assertFalse(eureka.isEnabled());
        assertEquals("true", eureka.getProperty("self-registration"));
        assertEquals("hazelcast", eureka.getProperty("namespace"));
    }

    private void assertDiscoveryConfig(DiscoveryConfig discoveryConfig) {
        assertTrue(discoveryConfig.getDiscoveryServiceProvider() instanceof DummyDiscoveryServiceProvider);
        assertTrue(discoveryConfig.getNodeFilter() instanceof DummyNodeFilter);
        List<DiscoveryStrategyConfig> discoveryStrategyConfigs
                = (List<DiscoveryStrategyConfig>) discoveryConfig.getDiscoveryStrategyConfigs();
        assertEquals(2, discoveryStrategyConfigs.size());
        DiscoveryStrategyConfig discoveryStrategyConfig = discoveryStrategyConfigs.get(0);
        assertTrue(discoveryStrategyConfig.getDiscoveryStrategyFactory() instanceof DummyDiscoveryStrategyFactory);
        assertEquals(3, discoveryStrategyConfig.getProperties().size());
        assertEquals("foo", discoveryStrategyConfig.getProperties().get("key-string"));
        assertEquals("123", discoveryStrategyConfig.getProperties().get("key-int"));
        assertEquals("true", discoveryStrategyConfig.getProperties().get("key-boolean"));

        DiscoveryStrategyConfig discoveryStrategyConfig2 = discoveryStrategyConfigs.get(1);
        assertEquals(DummyDiscoveryStrategy.class.getName(), discoveryStrategyConfig2.getClassName());
        assertEquals(1, discoveryStrategyConfig2.getProperties().size());
        assertEquals("foo2", discoveryStrategyConfig2.getProperties().get("key-string"));
    }

    @Test
    public void testProperties() {
        Properties properties = config.getProperties();
        assertNotNull(properties);
        assertEquals("5", properties.get(MERGE_FIRST_RUN_DELAY_SECONDS.getName()));
        assertEquals("5", properties.get(MERGE_NEXT_RUN_DELAY_SECONDS.getName()));
        assertEquals("277", properties.get(PARTITION_COUNT.getName()));

        Config config2 = instance.getConfig();
        Properties properties2 = config2.getProperties();
        assertNotNull(properties2);
        assertEquals("5", properties2.get(MERGE_FIRST_RUN_DELAY_SECONDS.getName()));
        assertEquals("5", properties2.get(MERGE_NEXT_RUN_DELAY_SECONDS.getName()));
        assertEquals("277", properties2.get(PARTITION_COUNT.getName()));
    }

    @Test
    public void testInstance() {
        assertNotNull(instance);
        Set<Member> members = instance.getCluster().getMembers();
        assertEquals(1, members.size());
        Member member = members.iterator().next();
        InetSocketAddress inetSocketAddress = member.getSocketAddress();
        assertEquals(5700, inetSocketAddress.getPort());
        assertEquals("test-instance", config.getInstanceName());
        assertEquals("HAZELCAST_ENTERPRISE_LICENSE_KEY", config.getLicenseKey());
        assertEquals(277, instance.getPartitionService().getPartitions().size());
    }

    @Test
    public void testHazelcastInstances() {
        assertNotNull(map1);
        assertNotNull(map2);
        assertNotNull(multiMap);
        assertNotNull(replicatedMap);
        assertNotNull(queue);
        assertNotNull(topic);
        assertNotNull(set);
        assertNotNull(list);
        assertNotNull(executorService);
        assertNotNull(idGenerator);
        assertNotNull(flakeIdGenerator);
        assertNotNull(atomicLong);
        assertNotNull(atomicReference);
        assertNotNull(countDownLatch);
        assertNotNull(semaphore);
        assertNotNull(lock);
        assertNotNull(pnCounter);
        assertEquals("map1", map1.getName());
        assertEquals("map2", map2.getName());
        assertEquals("testMultimap", multiMap.getName());
        assertEquals("replicatedMap", replicatedMap.getName());
        assertEquals("testQ", queue.getName());
        assertEquals("testTopic", topic.getName());
        assertEquals("set", set.getName());
        assertEquals("list", list.getName());
        assertEquals("idGenerator", idGenerator.getName());
        assertEquals("flakeIdGenerator", flakeIdGenerator.getName());
        assertEquals("testAtomicLong", atomicLong.getName());
        assertEquals("testAtomicReference", atomicReference.getName());
        assertEquals("countDownLatch", countDownLatch.getName());
        assertEquals("semaphore", semaphore.getName());
    }

    @Test
    public void testWanReplicationConfig() {
        WanReplicationConfig wcfg = config.getWanReplicationConfig("testWan");
        assertNotNull(wcfg);

        WanPublisherConfig publisherConfig = wcfg.getWanPublisherConfigs().get(0);
        assertEquals("tokyo", publisherConfig.getGroupName());
        assertEquals("com.hazelcast.enterprise.wan.replication.WanBatchReplication", publisherConfig.getClassName());
        assertEquals(WANQueueFullBehavior.THROW_EXCEPTION, publisherConfig.getQueueFullBehavior());
        assertEquals(WanPublisherState.STOPPED, publisherConfig.getInitialPublisherState());
        assertEquals(1000, publisherConfig.getQueueCapacity());
        Map<String, Comparable> publisherProps = publisherConfig.getProperties();
        assertEquals("50", publisherProps.get("batch.size"));
        assertEquals("3000", publisherProps.get("batch.max.delay.millis"));
        assertEquals("false", publisherProps.get("snapshot.enabled"));
        assertEquals("5000", publisherProps.get("response.timeout.millis"));
        assertEquals(WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE.name(), publisherProps.get("ack.type"));
        assertEquals("pass", publisherProps.get("group.password"));

        WanPublisherConfig customPublisher = wcfg.getWanPublisherConfigs().get(1);
        assertEquals("istanbul", customPublisher.getGroupName());
        assertEquals("com.hazelcast.wan.custom.CustomPublisher", customPublisher.getClassName());
        assertEquals(WANQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE, customPublisher.getQueueFullBehavior());
        Map<String, Comparable> customPublisherProps = customPublisher.getProperties();
        assertEquals("prop.publisher", customPublisherProps.get("custom.prop.publisher"));
        assertEquals("5", customPublisherProps.get("discovery.period"));
        assertEquals("2", customPublisherProps.get("maxEndpoints"));
        assertAwsConfig(customPublisher.getAwsConfig());
        assertGcpConfig(customPublisher.getGcpConfig());
        assertAzureConfig(customPublisher.getAzureConfig());
        assertKubernetesConfig(customPublisher.getKubernetesConfig());
        assertEurekaConfig(customPublisher.getEurekaConfig());
        assertDiscoveryConfig(customPublisher.getDiscoveryConfig());

        WanPublisherConfig publisherPlaceHolderConfig = wcfg.getWanPublisherConfigs().get(2);
        assertEquals(5000, publisherPlaceHolderConfig.getQueueCapacity());

        WanConsumerConfig consumerConfig = wcfg.getWanConsumerConfig();
        assertEquals("com.hazelcast.wan.custom.WanConsumer", consumerConfig.getClassName());
        Map<String, Comparable> consumerProps = consumerConfig.getProperties();
        assertEquals("prop.consumer", consumerProps.get("custom.prop.consumer"));
        assertTrue(consumerConfig.isPersistWanReplicatedData());
    }

    @Test
    public void testWanConsumerWithPersistDataFalse() {
        WanReplicationConfig config2 = config.getWanReplicationConfig("testWan2");
        WanConsumerConfig consumerConfig2 = config2.getWanConsumerConfig();
        assertInstanceOf(DummyWanConsumer.class, consumerConfig2.getImplementation());
        assertFalse(consumerConfig2.isPersistWanReplicatedData());
    }

    @Test
    public void testNoWanConsumerClass() {
        WanReplicationConfig config2 = config.getWanReplicationConfig("testWan3");
        WanConsumerConfig consumerConfig2 = config2.getWanConsumerConfig();
        assertFalse(consumerConfig2.isPersistWanReplicatedData());
    }

    @Test
    public void testWanReplicationSyncConfig() {
        final WanReplicationConfig wcfg = config.getWanReplicationConfig("testWan2");
        final WanConsumerConfig consumerConfig = wcfg.getWanConsumerConfig();
        final Map<String, Comparable> consumerProps = new HashMap<String, Comparable>();
        consumerProps.put("custom.prop.consumer", "prop.consumer");
        consumerConfig.setProperties(consumerProps);
        assertInstanceOf(DummyWanConsumer.class, consumerConfig.getImplementation());
        assertEquals("prop.consumer", consumerConfig.getProperties().get("custom.prop.consumer"));
        assertFalse(consumerConfig.isPersistWanReplicatedData());

        final List<WanPublisherConfig> publisherConfigs = wcfg.getWanPublisherConfigs();
        assertNotNull(publisherConfigs);
        assertEquals(1, publisherConfigs.size());

        final WanPublisherConfig publisherConfig = publisherConfigs.get(0);
        assertEquals("tokyo", publisherConfig.getGroupName());
        assertEquals("PublisherClassName", publisherConfig.getClassName());

        final WanSyncConfig wanSyncConfig = publisherConfig.getWanSyncConfig();
        assertNotNull(wanSyncConfig);
        assertEquals(ConsistencyCheckStrategy.MERKLE_TREES, wanSyncConfig.getConsistencyCheckStrategy());
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
        for (MemberGroupConfig mgc : pgc.getMemberGroupConfigs()) {
            assertEquals(2, mgc.getInterfaces().size());
        }
    }

    @Test
    public void testCRDTReplicationConfig() {
        CRDTReplicationConfig replicationConfig = config.getCRDTReplicationConfig();
        assertEquals(10, replicationConfig.getMaxConcurrentReplicationTargets());
        assertEquals(2000, replicationConfig.getReplicationPeriodMillis());
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
        assertEquals(2, managementCenterConfig.getUpdateInterval());
        assertTrue(managementCenterConfig.getMutualAuthConfig().isEnabled());
        assertEquals(1, managementCenterConfig.getMutualAuthConfig().getProperties().size());
        assertEquals("who.let.the.cat.out.class", managementCenterConfig.getMutualAuthConfig().getFactoryClassName());
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
        assertFalse(serializationConfig.isCheckClassDefErrors());
        assertEquals(13, serializationConfig.getPortableVersion());

        Map<Integer, String> dataSerializableFactoryClasses
                = serializationConfig.getDataSerializableFactoryClasses();
        assertFalse(dataSerializableFactoryClasses.isEmpty());
        assertEquals(DummyDataSerializableFactory.class.getName(), dataSerializableFactoryClasses.get(1));

        Map<Integer, DataSerializableFactory> dataSerializableFactories
                = serializationConfig.getDataSerializableFactories();
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

    @Test
    public void testNativeMemoryConfig() {
        NativeMemoryConfig nativeMemoryConfig = config.getNativeMemoryConfig();
        assertFalse(nativeMemoryConfig.isEnabled());
        assertEquals(MemoryUnit.MEGABYTES, nativeMemoryConfig.getSize().getUnit());
        assertEquals(256, nativeMemoryConfig.getSize().getValue());
        assertEquals(20, nativeMemoryConfig.getPageSize());
        assertEquals(NativeMemoryConfig.MemoryAllocatorType.POOLED, nativeMemoryConfig.getAllocatorType());
        assertEquals(10.2, nativeMemoryConfig.getMetadataSpacePercentage(), 0.1);
        assertEquals(10, nativeMemoryConfig.getMinBlockSize());
    }

    @Test
    public void testReplicatedMapConfig() {
        assertNotNull(config);
        assertEquals(1, config.getReplicatedMapConfigs().size());

        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig("replicatedMap");
        assertNotNull(replicatedMapConfig);
        assertEquals("replicatedMap", replicatedMapConfig.getName());
        assertEquals(InMemoryFormat.OBJECT, replicatedMapConfig.getInMemoryFormat());
        assertEquals(200, replicatedMapConfig.getReplicationDelayMillis());
        assertEquals(16, replicatedMapConfig.getConcurrencyLevel());
        assertFalse(replicatedMapConfig.isAsyncFillup());
        assertFalse(replicatedMapConfig.isStatisticsEnabled());
        assertEquals("my-quorum", replicatedMapConfig.getQuorumName());

        MergePolicyConfig mergePolicyConfig = replicatedMapConfig.getMergePolicyConfig();
        assertNotNull(mergePolicyConfig);
        assertEquals("PassThroughMergePolicy", mergePolicyConfig.getPolicy());
        assertEquals(2342, mergePolicyConfig.getBatchSize());

        replicatedMapConfig.getListenerConfigs();
        for (ListenerConfig listener : replicatedMapConfig.getListenerConfigs()) {
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
    public void testQuorumConfig() {
        assertNotNull(config);
        assertEquals(3, config.getQuorumConfigs().size());
        QuorumConfig quorumConfig = config.getQuorumConfig("my-quorum");
        assertNotNull(quorumConfig);
        assertEquals("my-quorum", quorumConfig.getName());
        assertEquals("com.hazelcast.spring.DummyQuorumFunction", quorumConfig.getQuorumFunctionClassName());
        assertTrue(quorumConfig.isEnabled());
        assertEquals(2, quorumConfig.getSize());
        assertEquals(2, quorumConfig.getListenerConfigs().size());
        assertEquals(QuorumType.READ, quorumConfig.getType());
        assertEquals("com.hazelcast.spring.DummyQuorumListener", quorumConfig.getListenerConfigs().get(0).getClassName());
        assertNotNull(quorumConfig.getListenerConfigs().get(1).getImplementation());
    }

    @Test
    public void testProbabilisticQuorumConfig() {
        QuorumConfig probabilisticQuorumConfig = config.getQuorumConfig("probabilistic-quorum");
        assertNotNull(probabilisticQuorumConfig);
        assertEquals("probabilistic-quorum", probabilisticQuorumConfig.getName());
        assertNotNull(probabilisticQuorumConfig.getQuorumFunctionImplementation());
        assertInstanceOf(ProbabilisticQuorumFunction.class, probabilisticQuorumConfig.getQuorumFunctionImplementation());
        assertTrue(probabilisticQuorumConfig.isEnabled());
        assertEquals(3, probabilisticQuorumConfig.getSize());
        assertEquals(2, probabilisticQuorumConfig.getListenerConfigs().size());
        assertEquals(QuorumType.READ_WRITE, probabilisticQuorumConfig.getType());
        assertEquals("com.hazelcast.spring.DummyQuorumListener",
                probabilisticQuorumConfig.getListenerConfigs().get(0).getClassName());
        assertNotNull(probabilisticQuorumConfig.getListenerConfigs().get(1).getImplementation());
        ProbabilisticQuorumFunction quorumFunction =
                (ProbabilisticQuorumFunction) probabilisticQuorumConfig.getQuorumFunctionImplementation();
        assertEquals(11, quorumFunction.getSuspicionThreshold(), 0.001d);
        assertEquals(31415, quorumFunction.getAcceptableHeartbeatPauseMillis());
        assertEquals(42, quorumFunction.getMaxSampleSize());
        assertEquals(77123, quorumFunction.getHeartbeatIntervalMillis());
        assertEquals(1000, quorumFunction.getMinStdDeviationMillis());
    }

    @Test
    public void testRecentlyActiveQuorumConfig() {
        QuorumConfig recentlyActiveQuorumConfig = config.getQuorumConfig("recently-active-quorum");
        assertNotNull(recentlyActiveQuorumConfig);
        assertEquals("recently-active-quorum", recentlyActiveQuorumConfig.getName());
        assertNotNull(recentlyActiveQuorumConfig.getQuorumFunctionImplementation());
        assertInstanceOf(RecentlyActiveQuorumFunction.class, recentlyActiveQuorumConfig.getQuorumFunctionImplementation());
        assertTrue(recentlyActiveQuorumConfig.isEnabled());
        assertEquals(5, recentlyActiveQuorumConfig.getSize());
        assertEquals(QuorumType.READ_WRITE, recentlyActiveQuorumConfig.getType());
        RecentlyActiveQuorumFunction quorumFunction =
                (RecentlyActiveQuorumFunction) recentlyActiveQuorumConfig.getQuorumFunctionImplementation();
        assertEquals(5123, quorumFunction.getHeartbeatToleranceMillis());
    }

    @Test
    public void testFullQueryCacheConfig() {
        MapConfig mapConfig = config.getMapConfig("map-with-query-cache");
        QueryCacheConfig queryCacheConfig = mapConfig.getQueryCacheConfigs().get(0);
        EntryListenerConfig entryListenerConfig = queryCacheConfig.getEntryListenerConfigs().get(0);

        assertTrue(entryListenerConfig.isIncludeValue());
        assertFalse(entryListenerConfig.isLocal());

        assertEquals("com.hazelcast.spring.DummyEntryListener", entryListenerConfig.getClassName());
        assertFalse(queryCacheConfig.isIncludeValue());

        assertEquals("my-query-cache-1", queryCacheConfig.getName());
        assertEquals(12, queryCacheConfig.getBatchSize());
        assertEquals(33, queryCacheConfig.getBufferSize());
        assertEquals(12, queryCacheConfig.getDelaySeconds());
        assertEquals(InMemoryFormat.OBJECT, queryCacheConfig.getInMemoryFormat());
        assertTrue(queryCacheConfig.isCoalesce());
        assertFalse(queryCacheConfig.isPopulate());
        assertIndexesEqual(queryCacheConfig);
        assertEquals("__key > 12", queryCacheConfig.getPredicateConfig().getSql());
        assertEquals(EvictionPolicy.LRU, queryCacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals(EvictionConfig.MaxSizePolicy.ENTRY_COUNT, queryCacheConfig.getEvictionConfig().getMaximumSizePolicy());
        assertEquals(111, queryCacheConfig.getEvictionConfig().getSize());
    }

    private void assertIndexesEqual(QueryCacheConfig queryCacheConfig) {
        for (MapIndexConfig mapIndexConfig : queryCacheConfig.getIndexConfigs()) {
            assertEquals("name", mapIndexConfig.getAttribute());
            assertFalse(mapIndexConfig.isOrdered());
        }
    }

    @Test
    public void testMapNativeMaxSizePolicy() {
        MapConfig mapConfig = config.getMapConfig("map-with-native-max-size-policy");
        MaxSizeConfig maxSizeConfig = mapConfig.getMaxSizeConfig();

        assertEquals(MaxSizeConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE, maxSizeConfig.getMaxSizePolicy());
    }

    @Test
    public void testHotRestart() {
        File dir = new File("/mnt/hot-restart/");
        File hotBackupDir = new File("/mnt/hot-backup/");
        HotRestartPersistenceConfig hotRestartPersistenceConfig = config.getHotRestartPersistenceConfig();

        assertTrue(hotRestartPersistenceConfig.isEnabled());
        assertEquals(dir.getAbsolutePath(), hotRestartPersistenceConfig.getBaseDir().getAbsolutePath());
        assertEquals(hotBackupDir.getAbsolutePath(), hotRestartPersistenceConfig.getBackupDir().getAbsolutePath());
        assertEquals(1111, hotRestartPersistenceConfig.getValidationTimeoutSeconds());
        assertEquals(2222, hotRestartPersistenceConfig.getDataLoadTimeoutSeconds());
        assertEquals(PARTIAL_RECOVERY_MOST_COMPLETE, hotRestartPersistenceConfig.getClusterDataRecoveryPolicy());
    }

    @Test
    public void testMapEvictionPolicies() {
        assertEquals(EvictionPolicy.LFU, config.getMapConfig("lfuEvictionMap").getEvictionPolicy());
        assertEquals(EvictionPolicy.LRU, config.getMapConfig("lruEvictionMap").getEvictionPolicy());
        assertEquals(EvictionPolicy.NONE, config.getMapConfig("noneEvictionMap").getEvictionPolicy());
        assertEquals(EvictionPolicy.RANDOM, config.getMapConfig("randomEvictionMap").getEvictionPolicy());
    }

    @Test
    public void testMemberNearCacheEvictionPolicies() {
        assertEquals(EvictionPolicy.LFU, getNearCacheEvictionPolicy("lfuNearCacheEvictionMap", config));
        assertEquals(EvictionPolicy.LRU, getNearCacheEvictionPolicy("lruNearCacheEvictionMap", config));
        assertEquals(EvictionPolicy.NONE, getNearCacheEvictionPolicy("noneNearCacheEvictionMap", config));
        assertEquals(EvictionPolicy.RANDOM, getNearCacheEvictionPolicy("randomNearCacheEvictionMap", config));
    }

    private EvictionPolicy getNearCacheEvictionPolicy(String mapName, Config config) {
        return config.getMapConfig(mapName).getNearCacheConfig().getEvictionConfig().getEvictionPolicy();
    }

    @Test
    public void testMapEvictionPolicyClassName() {
        MapConfig mapConfig = config.getMapConfig("mapWithMapEvictionPolicyClassName");
        String expectedComparatorClassName = "com.hazelcast.map.eviction.LRUEvictionPolicy";

        assertEquals(expectedComparatorClassName, mapConfig.getMapEvictionPolicy().getClass().getName());
    }

    @Test
    public void testMapEvictionPolicyImpl() {
        MapConfig mapConfig = config.getMapConfig("mapWithMapEvictionPolicyImpl");

        assertEquals(DummyMapEvictionPolicy.class, mapConfig.getMapEvictionPolicy().getClass());
    }

    @Test
    public void testWhenBothMapEvictionPolicyClassNameAndEvictionPolicySet() {
        MapConfig mapConfig = config.getMapConfig("mapBothMapEvictionPolicyClassNameAndEvictionPolicy");
        String expectedComparatorClassName = "com.hazelcast.map.eviction.LRUEvictionPolicy";

        assertEquals(expectedComparatorClassName, mapConfig.getMapEvictionPolicy().getClass().getName());
    }

    @Test
    public void testMapEventJournalConfigIsWellParsed() {
        EventJournalConfig journalConfig = config.getMapEventJournalConfig("mapName");

        assertTrue(journalConfig.isEnabled());
        assertEquals(123, journalConfig.getCapacity());
        assertEquals(321, journalConfig.getTimeToLiveSeconds());
    }

    @Test
    public void testCacheEventJournalConfigIsWellParsed() {
        EventJournalConfig journalConfig = config.getCacheEventJournalConfig("cacheName");

        assertTrue(journalConfig.isEnabled());
        assertEquals(123, journalConfig.getCapacity());
        assertEquals(321, journalConfig.getTimeToLiveSeconds());
    }

    @Test
    public void testMapMerkleTreeConfigIsWellParsed() {
        MerkleTreeConfig treeConfig = config.getMapMerkleTreeConfig("mapName");

        assertTrue(treeConfig.isEnabled());
        assertEquals(15, treeConfig.getDepth());
    }

    @Test
    public void testExplicitPortCountConfiguration() {
        int portCount = instance.getConfig().getNetworkConfig().getPortCount();

        assertEquals(42, portCount);
    }

    @Test
    public void testJavaSerializationFilterConfig() {
        JavaSerializationFilterConfig filterConfig = config.getSerializationConfig().getJavaSerializationFilterConfig();
        assertNotNull(filterConfig);
        assertTrue(filterConfig.isDefaultsDisabled());

        ClassFilter blacklist = filterConfig.getBlacklist();
        assertNotNull(blacklist);
        assertEquals(1, blacklist.getClasses().size());
        assertTrue(blacklist.getClasses().contains("com.acme.app.BeanComparator"));
        assertEquals(0, blacklist.getPackages().size());
        Set<String> prefixes = blacklist.getPrefixes();
        assertTrue(prefixes.contains("a.dangerous.package."));
        assertTrue(prefixes.contains("justaprefix"));
        assertEquals(2, prefixes.size());

        ClassFilter whitelist = filterConfig.getWhitelist();
        assertNotNull(whitelist);
        assertEquals(2, whitelist.getClasses().size());
        assertTrue(whitelist.getClasses().contains("java.lang.String"));
        assertTrue(whitelist.getClasses().contains("example.Foo"));
        assertEquals(2, whitelist.getPackages().size());
        assertTrue(whitelist.getPackages().contains("com.acme.app"));
        assertTrue(whitelist.getPackages().contains("com.acme.app.subpkg"));
    }
}
