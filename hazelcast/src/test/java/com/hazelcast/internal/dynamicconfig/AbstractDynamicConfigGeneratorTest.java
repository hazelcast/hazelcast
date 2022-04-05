/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.collection.QueueStore;
import com.hazelcast.collection.QueueStoreFactory;
import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.CachePartitionLostListenerConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CacheSimpleEntryListenerConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigCompatibilityChecker;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.DataPersistenceConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.DiskTierConfig;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.MemoryTierConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.config.MetadataPolicy;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TieredStoreConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanQueueFullBehavior;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapStore;
import com.hazelcast.map.MapStoreFactory;
import com.hazelcast.memory.Capacity;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.ringbuffer.RingbufferStore;
import com.hazelcast.ringbuffer.RingbufferStoreFactory;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.topic.TopicOverloadPolicy;
import com.hazelcast.wan.WanPublisherState;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig.ExpiryPolicyType.ACCESSED;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class AbstractDynamicConfigGeneratorTest extends HazelcastTestSupport {

    // MAP

    @Test
    public void testAttributesConfigWithStoreClass() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                .setWriteDelaySeconds(10)
                .setClassName("className")
                .setWriteCoalescing(false)
                .setWriteBatchSize(500)
                .setProperty("key", "value");

        testMap(mapStoreConfig);
    }

    @Test
    public void testAttributesConfigWithStoreImplementation() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                .setWriteDelaySeconds(10)
                .setImplementation(new TestMapStore())
                .setWriteCoalescing(true)
                .setWriteBatchSize(500)
                .setProperty("key", "value");

        testMap(mapStoreConfig);
    }

    @Test
    public void testAttributesConfigWithStoreFactory() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                .setWriteDelaySeconds(10)
                .setWriteCoalescing(true)
                .setWriteBatchSize(500)
                .setFactoryClassName("factoryClassName")
                .setProperty("key", "value");

        testMap(mapStoreConfig);
    }

    @Test
    public void testAttributesConfigWithStoreFactoryImplementation() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                .setWriteDelaySeconds(10)
                .setWriteCoalescing(true)
                .setWriteBatchSize(500)
                .setFactoryImplementation((MapStoreFactory<Object, Object>) (mapName, properties) -> null)
                .setProperty("key", "value");

        testMap(mapStoreConfig);
    }

    @Test
    public void testMapWithoutMerkleTreeConfig() {
        MapConfig expectedConfig = newMapConfig()
                .setName("testMapWithoutMerkleTreeConfig");
        Config config = new Config()
                .addMapConfig(expectedConfig);

        Config decConfig = getNewConfigViaGenerator(config);
        MapConfig actualConfig = decConfig.getMapConfig("testMapWithoutMerkleTreeConfig");
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testMapWithEnabledMerkleTreeConfig() {
        MapConfig expectedConfig = newMapConfig()
                .setName("testMapWithEnabledMerkleTreeConfig");
        expectedConfig.getMerkleTreeConfig().setEnabled(true).setDepth(13);
        Config config = new Config()
                .addMapConfig(expectedConfig);

        Config decConfig = getNewConfigViaGenerator(config);
        MapConfig actualConfig = decConfig.getMapConfig("testMapWithEnabledMerkleTreeConfig");
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testMapWithDisabledMerkleTreeConfig() {
        MapConfig expectedConfig = newMapConfig()
                .setName("testMapWithEnabledMerkleTreeConfig");
        expectedConfig.getMerkleTreeConfig().setEnabled(false).setDepth(13);
        Config config = new Config()
                .addMapConfig(expectedConfig);

        Config decConfig = getNewConfigViaGenerator(config);
        MapConfig actualConfig = decConfig.getMapConfig("testMapWithEnabledMerkleTreeConfig");
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testMapNearCacheConfig() {
        NearCacheConfig expectedConfig = new NearCacheConfig()
                .setName("nearCache")
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setMaxIdleSeconds(42)
                .setCacheLocalEntries(true)
                .setInvalidateOnChange(true)
                .setLocalUpdatePolicy(NearCacheConfig.LocalUpdatePolicy.INVALIDATE)
                .setTimeToLiveSeconds(10)
                .setEvictionConfig(evictionConfig())
                .setSerializeKeys(true);

        MapConfig mapConfig = newMapConfig()
                .setName("nearCacheTest")
                .setNearCacheConfig(expectedConfig);

        Config config = new Config()
                .addMapConfig(mapConfig);

        Config decConfig = getNewConfigViaGenerator(config);

        NearCacheConfig actualConfig = decConfig.getMapConfig("nearCacheTest").getNearCacheConfig();
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testMapNearCacheEvictionConfig() {
        NearCacheConfig expectedConfig = new NearCacheConfig()
                .setName("nearCache");
        expectedConfig.getEvictionConfig().setSize(23).setEvictionPolicy(EvictionPolicy.LRU);

        MapConfig mapConfig = newMapConfig()
                .setName("nearCacheTest")
                .setNearCacheConfig(expectedConfig);

        Config config = new Config()
                .addMapConfig(mapConfig);

        Config decConfig = getNewConfigViaGenerator(config);

        NearCacheConfig actualConfig = decConfig.getMapConfig("nearCacheTest").getNearCacheConfig();
        assertEquals(23, actualConfig.getEvictionConfig().getSize());
        assertEquals("LRU", actualConfig.getEvictionConfig().getEvictionPolicy().name());
        assertEquals(expectedConfig, actualConfig);
    }

    // CACHE

    @Test
    public void testCacheAttributes() {
        CacheSimpleConfig expectedConfig = new CacheSimpleConfig()
                .setName("testCache")
                .setEvictionConfig(evictionConfig())
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setCacheLoader("cacheLoader")
                .setCacheWriter("cacheWriter")
                .setExpiryPolicyFactoryConfig(new CacheSimpleConfig.ExpiryPolicyFactoryConfig("expiryPolicyFactory"))
                .setManagementEnabled(true)
                .setStatisticsEnabled(true)
                .setKeyType("keyType")
                .setValueType("valueType")
                .setReadThrough(true)
                .setDataPersistenceConfig(dataPersistenceConfig())
                .setEventJournalConfig(eventJournalConfig())
                .setCacheEntryListeners(singletonList(cacheSimpleEntryListenerConfig()))
                .setWriteThrough(true)
                .setPartitionLostListenerConfigs(singletonList(
                        new CachePartitionLostListenerConfig("partitionLostListener")))
                .setSplitBrainProtectionName("testSplitBrainProtection");

        expectedConfig.getMergePolicyConfig().setPolicy("HigherHitsMergePolicy").setBatchSize(99);
        expectedConfig.setDisablePerEntryInvalidationEvents(true);
        expectedConfig.setWanReplicationRef(wanReplicationRef());

        Config config = new Config()
                .addCacheConfig(expectedConfig);

        Config decConfig = getNewConfigViaGenerator(config);

        CacheSimpleConfig actualConfig = decConfig.getCacheConfig("testCache");
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testCacheFactoryAttributes() {
        CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig = new CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig(ACCESSED,
                new CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig(10, SECONDS));

        CacheSimpleConfig expectedConfig = new CacheSimpleConfig()
                .setName("testCache")
                .setCacheLoaderFactory("cacheLoaderFactory")
                .setCacheWriterFactory("cacheWriterFactory")
                .setExpiryPolicyFactory("expiryPolicyFactory")
                .setCacheEntryListeners(singletonList(cacheSimpleEntryListenerConfig()))
                .setExpiryPolicyFactoryConfig(new CacheSimpleConfig.ExpiryPolicyFactoryConfig(timedExpiryPolicyFactoryConfig))
                .setPartitionLostListenerConfigs(singletonList(
                        new CachePartitionLostListenerConfig("partitionLostListener")));

        expectedConfig.getMergePolicyConfig().setPolicy("mergePolicy");
        expectedConfig.setDisablePerEntryInvalidationEvents(true);

        Config config = new Config()
                .addCacheConfig(expectedConfig);

        Config decConfig = getNewConfigViaGenerator(config);

        CacheSimpleConfig actualConfig = decConfig.getCacheConfig("testCache");
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testCacheSplitBrainProtectionRef() {
        CacheSimpleConfig expectedConfig = new CacheSimpleConfig()
                .setName("testCache")
                .setSplitBrainProtectionName("testSplitBrainProtection");

        Config config = new Config()
                .addCacheConfig(expectedConfig);

        Config decConfig = getNewConfigViaGenerator(config);

        CacheSimpleConfig actualConfig = decConfig.getCacheConfig("testCache");
        assertEquals("testSplitBrainProtection", actualConfig.getSplitBrainProtectionName());
    }

    // QUEUE

    @Test
    public void testQueueWithStoreClass() {
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setClassName("className")
                .setEnabled(true)
                .setProperty("key", "value");

        testQueue(queueStoreConfig);
    }

    @Test
    public void testQueueWithStoreImplementation() {
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setStoreImplementation(new TestQueueStore())
                .setEnabled(true)
                .setProperty("key", "value");

        testQueue(queueStoreConfig);
    }

    @Test
    public void testQueueWithStoreFactory() {
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setFactoryClassName("factoryClassName")
                .setEnabled(true)
                .setProperty("key", "value");

        testQueue(queueStoreConfig);
    }

    @Test
    public void testQueueWithStoreFactoryImplementation() {
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setFactoryImplementation(new TestQueueStoreFactory())
                .setEnabled(true)
                .setProperty("key", "value");

        testQueue(queueStoreConfig);
    }

    // LIST

    @Test
    public void testList() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(HigherHitsMergePolicy.class.getName())
                .setBatchSize(1234);

        ListConfig expectedConfig = new ListConfig("testList")
                .setMaxSize(10)
                .setStatisticsEnabled(true)
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setSplitBrainProtectionName("splitBrainProtection")
                .setMergePolicyConfig(mergePolicyConfig)
                .setItemListenerConfigs(singletonList(new ItemListenerConfig("java.Listener", true)));

        Config config = new Config()
                .addListConfig(expectedConfig);

        Config decConfig = getNewConfigViaGenerator(config);

        ListConfig actualConfig = decConfig.getListConfig("testList");
        assertEquals(expectedConfig, actualConfig);
    }

    // LIST

    @Test
    public void testSet() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(LatestUpdateMergePolicy.class.getName())
                .setBatchSize(1234);

        SetConfig expectedConfig = new SetConfig("testSet")
                .setMaxSize(10)
                .setStatisticsEnabled(true)
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setSplitBrainProtectionName("splitBrainProtection")
                .setMergePolicyConfig(mergePolicyConfig)
                .setItemListenerConfigs(singletonList(new ItemListenerConfig("java.Listener", true)));

        Config config = new Config()
                .addSetConfig(expectedConfig);

        Config decConfig = getNewConfigViaGenerator(config);

        SetConfig actualConfig = decConfig.getSetConfig("testSet");
        assertEquals(expectedConfig, actualConfig);
    }

    // MULTI MAP

    @Test
    public void testMultiMap() {
        MultiMapConfig expectedConfig = new MultiMapConfig()
                .setName("testMultiMap")
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST)
                .setBinary(true)
                .setStatisticsEnabled(true)
                .setSplitBrainProtectionName("splitBrainProtection")
                .setEntryListenerConfigs(singletonList(new EntryListenerConfig("java.Listener", true, true)));

        Config config = new Config()
                .addMultiMapConfig(expectedConfig);

        Config decConfig = getNewConfigViaGenerator(config);

        MultiMapConfig actualConfig = decConfig.getMultiMapConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testMultiMapConfig() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(DiscardMergePolicy.class.getSimpleName())
                .setBatchSize(2342);

        MultiMapConfig multiMapConfig = new MultiMapConfig()
                .setName("myMultiMap")
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setBinary(false)
                .setMergePolicyConfig(mergePolicyConfig);

        Config config = new Config().addMultiMapConfig(multiMapConfig);
        Config decConfig = getNewConfigViaGenerator(config);

        assertEquals(multiMapConfig, decConfig.getMultiMapConfig("myMultiMap"));
    }

    // REPLICATED MAP

    @Test
    public void testReplicatedMapConfigGenerator() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy("PassThroughMergePolicy")
                .setBatchSize(1234);

        ReplicatedMapConfig replicatedMapConfig = new ReplicatedMapConfig()
                .setName("replicated-map-name")
                .setStatisticsEnabled(false)
                .setSplitBrainProtectionName("splitBrainProtection")
                .setMergePolicyConfig(mergePolicyConfig)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .addEntryListenerConfig(new EntryListenerConfig("com.hazelcast.entrylistener", false, false))
                .addEntryListenerConfig(new EntryListenerConfig("com.hazelcast.entrylistener2", true, false));

        replicatedMapConfig.setAsyncFillup(true);

        Config config = new Config()
                .addReplicatedMapConfig(replicatedMapConfig);

        Config decConfig = getNewConfigViaGenerator(config);

        ReplicatedMapConfig decReplicatedMapConfig = decConfig.getReplicatedMapConfig("replicated-map-name");
        MergePolicyConfig actualMergePolicyConfig = decReplicatedMapConfig.getMergePolicyConfig();
        assertEquals("replicated-map-name", decReplicatedMapConfig.getName());
        assertFalse(decReplicatedMapConfig.isStatisticsEnabled());
        assertEquals("com.hazelcast.entrylistener", decReplicatedMapConfig.getListenerConfigs().get(0).getClassName());
        assertEquals("splitBrainProtection", decReplicatedMapConfig.getSplitBrainProtectionName());
        assertEquals(InMemoryFormat.NATIVE, decReplicatedMapConfig.getInMemoryFormat());
        assertTrue(decReplicatedMapConfig.isAsyncFillup());
        assertEquals("PassThroughMergePolicy", actualMergePolicyConfig.getPolicy());
        assertEquals(1234, actualMergePolicyConfig.getBatchSize());
        assertEquals(replicatedMapConfig, decReplicatedMapConfig);
    }

    // RINGBUFFER

    @Test
    public void testRingbufferWithStoreClass() {
        RingbufferStoreConfig ringbufferStoreConfig = new RingbufferStoreConfig()
                .setEnabled(true)
                .setClassName("ClassName")
                .setProperty("p1", "v1")
                .setProperty("p2", "v2")
                .setProperty("p3", "v3");

        testRingbuffer(ringbufferStoreConfig);
    }

    @Test
    public void testRingbufferWithStoreImplementation() {
        RingbufferStoreConfig ringbufferStoreConfig = new RingbufferStoreConfig()
                .setEnabled(true)
                .setStoreImplementation(new TestRingbufferStore())
                .setProperty("p1", "v1")
                .setProperty("p2", "v2")
                .setProperty("p3", "v3");

        testRingbuffer(ringbufferStoreConfig);
    }

    @Test
    public void testRingbufferWithStoreFactory() {
        RingbufferStoreConfig ringbufferStoreConfig = new RingbufferStoreConfig()
                .setEnabled(true)
                .setFactoryClassName("FactoryClassName")
                .setProperty("p1", "v1")
                .setProperty("p2", "v2")
                .setProperty("p3", "v3");

        testRingbuffer(ringbufferStoreConfig);
    }

    @Test
    public void testRingbufferWithStoreFactoryImplementation() {
        RingbufferStoreConfig ringbufferStoreConfig = new RingbufferStoreConfig()
                .setEnabled(true)
                .setFactoryImplementation(new TestRingbufferStoreFactory())
                .setProperty("p1", "v1")
                .setProperty("p2", "v2")
                .setProperty("p3", "v3");

        testRingbuffer(ringbufferStoreConfig);
    }

    // TOPIC

    @Test
    public void testTopicGlobalOrdered() {
        Config cfg = new Config();

        TopicConfig expectedConfig = new TopicConfig()
                .setName("TestTopic")
                .setGlobalOrderingEnabled(true)
                .setStatisticsEnabled(true)
                .setMessageListenerConfigs(singletonList(new ListenerConfig("foo.bar.Listener")));
        cfg.addTopicConfig(expectedConfig);

        TopicConfig actualConfig = getNewConfigViaGenerator(cfg).getTopicConfig("TestTopic");

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testTopicMultiThreaded() {
        String testTopic = "TestTopic";
        Config cfg = new Config();

        TopicConfig expectedConfig = new TopicConfig()
                .setName(testTopic)
                .setMultiThreadingEnabled(true)
                .setStatisticsEnabled(true)
                .setMessageListenerConfigs(singletonList(new ListenerConfig("foo.bar.Listener")));
        cfg.addTopicConfig(expectedConfig);

        TopicConfig actualConfig = getNewConfigViaGenerator(cfg).getTopicConfig(testTopic);

        assertEquals(expectedConfig, actualConfig);
    }

    // RELIABLE TOPIC

    @Test
    public void testReliableTopic() {
        String testTopic = "TestTopic";
        Config cfg = new Config();

        ReliableTopicConfig expectedConfig = new ReliableTopicConfig()
                .setName(testTopic)
                .setReadBatchSize(10)
                .setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK)
                .setStatisticsEnabled(true)
                .setMessageListenerConfigs(singletonList(new ListenerConfig("foo.bar.Listener")));

        cfg.addReliableTopicConfig(expectedConfig);

        ReliableTopicConfig actualConfig = getNewConfigViaGenerator(cfg).getReliableTopicConfig(testTopic);

        assertEquals(expectedConfig, actualConfig);
    }

    // EXECUTOR

    @Test
    public void testExecutor() {
        ExecutorConfig expectedConfig = new ExecutorConfig()
                .setName("testExecutor")
                .setStatisticsEnabled(true)
                .setPoolSize(10)
                .setQueueCapacity(100)
                .setSplitBrainProtectionName("splitBrainProtection");

        Config config = new Config()
                .addExecutorConfig(expectedConfig);

        Config decConfig = getNewConfigViaGenerator(config);

        ExecutorConfig actualConfig = decConfig.getExecutorConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    // DURABLE EXECUTOR

    @Test
    public void testDurableExecutor() {
        DurableExecutorConfig expectedConfig = new DurableExecutorConfig()
                .setName("testDurableExecutor")
                .setPoolSize(10)
                .setCapacity(100)
                .setDurability(2)
                .setStatisticsEnabled(false)
                .setSplitBrainProtectionName("splitBrainProtection");

        Config config = new Config()
                .addDurableExecutorConfig(expectedConfig);

        Config decConfig = getNewConfigViaGenerator(config);

        DurableExecutorConfig actualConfig = decConfig.getDurableExecutorConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    // SCHEDULED EXECUTOR

    @Test
    public void testScheduledExecutor() {
        Config cfg = new Config();

        ScheduledExecutorConfig scheduledExecutorConfig =
                new ScheduledExecutorConfig()
                        .setCapacity(1)
                        .setCapacityPolicy(ScheduledExecutorConfig.CapacityPolicy.PER_PARTITION)
                        .setDurability(2)
                        .setName("Existing")
                        .setPoolSize(3)
                        .setSplitBrainProtectionName("splitBrainProtection")
                        .setMergePolicyConfig(new MergePolicyConfig("JediPolicy", 23))
                        .setStatisticsEnabled(false);

        cfg.addScheduledExecutorConfig(scheduledExecutorConfig);

        ScheduledExecutorConfig defaultScheduledExecConfig = new ScheduledExecutorConfig();
        cfg.addScheduledExecutorConfig(defaultScheduledExecConfig);

        ScheduledExecutorConfig existing = getNewConfigViaGenerator(cfg).getScheduledExecutorConfig("Existing");
        assertEquals(scheduledExecutorConfig, existing);

        ScheduledExecutorConfig fallbacksToDefault = getNewConfigViaGenerator(cfg)
                .getScheduledExecutorConfig("NotExisting/Default");
        assertEquals(defaultScheduledExecConfig.getMergePolicyConfig(), fallbacksToDefault.getMergePolicyConfig());
        assertEquals(defaultScheduledExecConfig.getCapacity(), fallbacksToDefault.getCapacity());
        assertEquals(defaultScheduledExecConfig.getCapacityPolicy(), fallbacksToDefault.getCapacityPolicy());
        assertEquals(defaultScheduledExecConfig.getPoolSize(), fallbacksToDefault.getPoolSize());
        assertEquals(defaultScheduledExecConfig.getDurability(), fallbacksToDefault.getDurability());
        assertEquals(defaultScheduledExecConfig.isStatisticsEnabled(), fallbacksToDefault.isStatisticsEnabled());
    }

    // CARDINALITY ESTIMATOR

    @Test
    public void testCardinalityEstimator() {
        Config cfg = new Config();
        CardinalityEstimatorConfig estimatorConfig = new CardinalityEstimatorConfig()
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setName("Existing")
                .setSplitBrainProtectionName("splitBrainProtection")
                .setMergePolicyConfig(new MergePolicyConfig("DiscardMergePolicy", 14));
        cfg.addCardinalityEstimatorConfig(estimatorConfig);

        CardinalityEstimatorConfig defaultCardinalityEstConfig = new CardinalityEstimatorConfig();
        cfg.addCardinalityEstimatorConfig(defaultCardinalityEstConfig);

        CardinalityEstimatorConfig existing = getNewConfigViaGenerator(cfg).getCardinalityEstimatorConfig("Existing");
        assertEquals(estimatorConfig, existing);

        CardinalityEstimatorConfig fallbacksToDefault = getNewConfigViaGenerator(cfg)
                .getCardinalityEstimatorConfig("NotExisting/Default");
        assertEquals(defaultCardinalityEstConfig.getMergePolicyConfig(), fallbacksToDefault.getMergePolicyConfig());
        assertEquals(defaultCardinalityEstConfig.getBackupCount(), fallbacksToDefault.getBackupCount());
        assertEquals(defaultCardinalityEstConfig.getAsyncBackupCount(), fallbacksToDefault.getAsyncBackupCount());
        assertEquals(defaultCardinalityEstConfig.getSplitBrainProtectionName(), fallbacksToDefault.getSplitBrainProtectionName());
    }

    // PN COUNTER

    @Test
    public void testPNCounter() {
        PNCounterConfig expectedConfig = new PNCounterConfig()
                .setName("testPNCounter")
                .setReplicaCount(100)
                .setSplitBrainProtectionName("splitBrainProtection");

        Config config = new Config().addPNCounterConfig(expectedConfig);

        Config decConfig = getNewConfigViaGenerator(config);

        PNCounterConfig actualConfig = decConfig.getPNCounterConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    // FLAKE ID GENERATOR

    @Test
    public void testFlakeIdGeneratorConfigGenerator() {
        FlakeIdGeneratorConfig figConfig = new FlakeIdGeneratorConfig("flake-id-gen1")
                .setPrefetchCount(3)
                .setPrefetchValidityMillis(10L)
                .setEpochStart(1000000L)
                .setNodeIdOffset(30L)
                .setBitsSequence(2)
                .setBitsNodeId(3)
                .setAllowedFutureMillis(123L)
                .setStatisticsEnabled(false);

        Config config = new Config()
                .addFlakeIdGeneratorConfig(figConfig);

        Config decConfig = getNewConfigViaGenerator(config);

        FlakeIdGeneratorConfig decReplicatedConfig = decConfig.getFlakeIdGeneratorConfig("flake-id-gen1");
        assertEquals(figConfig, decReplicatedConfig);
    }

    // WAN REPLICATION

    @Test
    public void testWanConfig() {
        @SuppressWarnings("rawtypes")
        HashMap<String, Comparable> props = new HashMap<>();
        props.put("prop1", "val1");
        props.put("prop2", "val2");
        props.put("prop3", "val3");
        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig()
                .setName("testName")
                .setConsumerConfig(new WanConsumerConfig().setClassName("dummyClass").setProperties(props));
        WanBatchPublisherConfig batchPublisher = new WanBatchPublisherConfig()
                .setClusterName("dummyGroup")
                .setPublisherId("dummyPublisherId")
                .setSnapshotEnabled(false)
                .setInitialPublisherState(WanPublisherState.STOPPED)
                .setQueueCapacity(1000)
                .setBatchSize(500)
                .setBatchMaxDelayMillis(1000)
                .setResponseTimeoutMillis(60000)
                .setQueueFullBehavior(WanQueueFullBehavior.DISCARD_AFTER_MUTATION)
                .setAcknowledgeType(WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE)
                .setDiscoveryPeriodSeconds(20)
                .setMaxTargetEndpoints(100)
                .setMaxConcurrentInvocations(500)
                .setUseEndpointPrivateAddress(true)
                .setIdleMinParkNs(100)
                .setIdleMaxParkNs(1000)
                .setTargetEndpoints("a,b,c,d")
                .setAwsConfig(getDummyAwsConfig())
                .setDiscoveryConfig(getDummyDiscoveryConfig())
                .setEndpoint("WAN")
                .setProperties(props);

        batchPublisher.getSyncConfig()
                .setConsistencyCheckStrategy(ConsistencyCheckStrategy.MERKLE_TREES);

        WanCustomPublisherConfig customPublisher = new WanCustomPublisherConfig()
                .setPublisherId("dummyPublisherId")
                .setClassName("className")
                .setProperties(props);

        WanConsumerConfig wanConsumerConfig = new WanConsumerConfig()
                .setClassName("dummyClass")
                .setProperties(props)
                .setPersistWanReplicatedData(false);

        wanReplicationConfig.setConsumerConfig(wanConsumerConfig)
                .addBatchReplicationPublisherConfig(batchPublisher)
                .addCustomPublisherConfig(customPublisher);

        Config config = new Config().addWanReplicationConfig(wanReplicationConfig);
        Config decConfig = getNewConfigViaGenerator(config);

        ConfigCompatibilityChecker.checkWanConfigs(
                config.getWanReplicationConfigs(),
                decConfig.getWanReplicationConfigs());
    }

    // UTILITY - GENERATOR

    protected abstract Config getNewConfigViaGenerator(Config config);

    // UTILITY - TESTS

    private void testMap(MapStoreConfig mapStoreConfig) {
        AttributeConfig attrConfig = new AttributeConfig()
                .setName("power")
                .setExtractorClassName("com.car.PowerExtractor");

        EvictionConfig evictionConfig1 = new EvictionConfig()
                .setSize(10)
                .setMaxSizePolicy(MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE);

        IndexConfig indexConfig = new IndexConfig().addAttribute("attribute").setType(IndexType.SORTED);

        EntryListenerConfig listenerConfig = new EntryListenerConfig("com.hazelcast.entrylistener", false, false);

        EvictionConfig evictionConfig2 = new EvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE)
                .setSize(100)
                .setComparatorClassName("comparatorClassName")
                .setEvictionPolicy(EvictionPolicy.LRU);

        PredicateConfig predicateConfig1 = new PredicateConfig();
        predicateConfig1.setClassName("className");

        PredicateConfig predicateConfig2 = new PredicateConfig();
        predicateConfig2.setSql("sqlQuery");

        QueryCacheConfig queryCacheConfig1 = new QueryCacheConfig()
                .setName("queryCache1")
                .setPredicateConfig(predicateConfig1)
                .addEntryListenerConfig(listenerConfig)
                .setBatchSize(230)
                .setDelaySeconds(20)
                .setPopulate(false)
                .setBufferSize(8)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setEvictionConfig(evictionConfig2)
                .setIncludeValue(false)
                .setCoalesce(false)
                .addIndexConfig(indexConfig);

        QueryCacheConfig queryCacheConfig2 = new QueryCacheConfig()
                .setName("queryCache2")
                .setPredicateConfig(predicateConfig2)
                .addEntryListenerConfig(listenerConfig)
                .setBatchSize(500)
                .setDelaySeconds(10)
                .setPopulate(true)
                .setBufferSize(10)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setEvictionConfig(evictionConfig2)
                .setIncludeValue(true)
                .setCoalesce(true)
                .addIndexConfig(indexConfig);

        MapConfig expectedConfig = newMapConfig()
                .setName("carMap")
                .setEvictionConfig(evictionConfig1)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setMetadataPolicy(MetadataPolicy.CREATE_ON_UPDATE)
                .setMaxIdleSeconds(100)
                .setTimeToLiveSeconds(1000)
                .setCacheDeserializedValues(CacheDeserializedValues.ALWAYS)
                .setStatisticsEnabled(true)
                .setPerEntryStatsEnabled(false)
                .setReadBackupData(true)
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setMapStoreConfig(mapStoreConfig)
                .setWanReplicationRef(wanReplicationRef())
                .setPartitioningStrategyConfig(new PartitioningStrategyConfig("partitionStrategyClass"))
                .setMerkleTreeConfig(merkleTreeConfig())
                .setEventJournalConfig(eventJournalConfig())
                .setDataPersistenceConfig(dataPersistenceConfig())
                .addEntryListenerConfig(listenerConfig)
                .setIndexConfigs(singletonList(indexConfig))
                .addAttributeConfig(attrConfig)
                .setPartitionLostListenerConfigs(singletonList(
                        new MapPartitionLostListenerConfig("partitionLostListener"))
                );

        expectedConfig.setQueryCacheConfigs(asList(queryCacheConfig1, queryCacheConfig2));

        Config config = new Config()
                .addMapConfig(expectedConfig);

        Config decConfig = getNewConfigViaGenerator(config);

        MapConfig actualConfig = decConfig.getMapConfig("carMap");
        AttributeConfig decAttrConfig = actualConfig.getAttributeConfigs().get(0);
        assertEquals(attrConfig.getName(), decAttrConfig.getName());
        assertEquals(attrConfig.getExtractorClassName(), decAttrConfig.getExtractorClassName());
        ConfigCompatibilityChecker.checkMapConfig(expectedConfig, actualConfig);
    }

    private void testQueue(QueueStoreConfig queueStoreConfig) {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(DiscardMergePolicy.class.getSimpleName())
                .setBatchSize(1234);

        QueueConfig expectedConfig = new QueueConfig()
                .setName("testQueue")
                .setPriorityComparatorClassName("com.hazelcast.collection.impl.queue.model.PriorityElementComparator")
                .setMaxSize(10)
                .setStatisticsEnabled(true)
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setEmptyQueueTtl(1000)
                .setMergePolicyConfig(mergePolicyConfig)
                .setQueueStoreConfig(queueStoreConfig)
                .setItemListenerConfigs(singletonList(new ItemListenerConfig("java.Listener", true)));

        Config config = new Config()
                .addQueueConfig(expectedConfig);

        Config decConfig = getNewConfigViaGenerator(config);

        QueueConfig actualConfig = decConfig.getQueueConfig("testQueue");
        assertEquals("testQueue", actualConfig.getName());

        MergePolicyConfig decMergePolicyConfig = actualConfig.getMergePolicyConfig();
        assertEquals(DiscardMergePolicy.class.getSimpleName(), decMergePolicyConfig.getPolicy());
        assertEquals(1234, decMergePolicyConfig.getBatchSize());
        ConfigCompatibilityChecker.checkQueueConfig(expectedConfig, actualConfig);
    }

    private void testRingbuffer(RingbufferStoreConfig ringbufferStoreConfig) {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy("PassThroughMergePolicy")
                .setBatchSize(1234);
        RingbufferConfig expectedConfig = new RingbufferConfig("testRbConfig")
                .setBackupCount(1)
                .setAsyncBackupCount(2)
                .setCapacity(3)
                .setTimeToLiveSeconds(4)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setRingbufferStoreConfig(ringbufferStoreConfig)
                .setSplitBrainProtectionName("splitBrainProtection")
                .setMergePolicyConfig(mergePolicyConfig);

        Config config = new Config().addRingBufferConfig(expectedConfig);

        Config decConfig = getNewConfigViaGenerator(config);

        RingbufferConfig actualConfig = decConfig.getRingbufferConfig(expectedConfig.getName());
        ConfigCompatibilityChecker.checkRingbufferConfig(expectedConfig, actualConfig);
    }

    // UTILITY - CLASSES

    private static class TestMapStore implements MapStore<Object, Object> {
        @Override
        public void store(Object key, Object value) {
        }

        @Override
        public void storeAll(Map<Object, Object> map) {
        }

        @Override
        public void delete(Object key) {
        }

        @Override
        public void deleteAll(Collection<Object> keys) {
        }

        @Override
        public Object load(Object key) {
            return null;
        }

        @Override
        public Map<Object, Object> loadAll(Collection<Object> keys) {
            return null;
        }

        @Override
        public Iterable<Object> loadAllKeys() {
            return null;
        }
    }

    private static class TestQueueStore implements QueueStore<Object> {
        @Override
        public void store(Long key, Object value) {
        }

        @Override
        public void storeAll(Map<Long, Object> map) {
        }

        @Override
        public void delete(Long key) {
        }

        @Override
        public void deleteAll(Collection<Long> keys) {
        }

        @Override
        public Object load(Long key) {
            return null;
        }

        @Override
        public Map<Long, Object> loadAll(Collection<Long> keys) {
            return null;
        }

        @Override
        public Set<Long> loadAllKeys() {
            return null;
        }
    }

    private static class TestQueueStoreFactory implements QueueStoreFactory<Object> {
        @Override
        public QueueStore<Object> newQueueStore(String name, Properties properties) {
            return null;
        }
    }

    private static class TestRingbufferStore implements RingbufferStore<Object> {
        @Override
        public void store(long sequence, Object data) {
        }

        @Override
        public void storeAll(long firstItemSequence, Object[] items) {
        }

        @Override
        public Object load(long sequence) {
            return null;
        }

        @Override
        public long getLargestSequence() {
            return 0;
        }
    }

    private static class TestRingbufferStoreFactory implements RingbufferStoreFactory<Object> {
        @Override
        public RingbufferStore<Object> newRingbufferStore(String name, Properties properties) {
            return null;
        }
    }

    private static class TestDiscoveryStrategyFactory implements DiscoveryStrategyFactory {
        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return null;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger, @SuppressWarnings("rawtypes") Map<String, Comparable> properties) {
            return null;
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return null;
        }
    }

    // UTILITY - METHODS

    private static MapConfig newMapConfig() {
        return new MapConfig().setTieredStoreConfig(tieredStoreConfig());
    }

    private static TieredStoreConfig tieredStoreConfig() {
        MemoryTierConfig memTierConfig = new MemoryTierConfig()
                .setCapacity(Capacity.of(13401L, MemoryUnit.MEGABYTES));

        DiskTierConfig diskTierConfig = new DiskTierConfig()
                .setEnabled(true)
                .setDeviceName("device04");

        return new TieredStoreConfig()
                .setEnabled(true)
                .setMemoryTierConfig(memTierConfig)
                .setDiskTierConfig(diskTierConfig);
    }

    private static CacheSimpleEntryListenerConfig cacheSimpleEntryListenerConfig() {
        CacheSimpleEntryListenerConfig entryListenerConfig = new CacheSimpleEntryListenerConfig();
        entryListenerConfig.setCacheEntryListenerFactory("entryListenerFactory");
        entryListenerConfig.setSynchronous(true);
        entryListenerConfig.setOldValueRequired(true);
        entryListenerConfig.setCacheEntryEventFilterFactory("entryEventFilterFactory");
        return entryListenerConfig;
    }

    private static WanReplicationRef wanReplicationRef() {
        return new WanReplicationRef()
                .setName("wanReplication")
                .setMergePolicyClassName("HigherHitsMergePolicy")
                .setRepublishingEnabled(true)
                .setFilters(Arrays.asList("filter1", "filter2"));
    }

    private static DataPersistenceConfig dataPersistenceConfig() {
        return new DataPersistenceConfig()
                .setEnabled(true)
                .setFsync(true);
    }

    private static MerkleTreeConfig merkleTreeConfig() {
        return new MerkleTreeConfig()
                .setEnabled(true)
                .setDepth(15);
    }

    private static EventJournalConfig eventJournalConfig() {
        return new EventJournalConfig()
                .setEnabled(true)
                .setCapacity(123)
                .setTimeToLiveSeconds(321);
    }

    private static EvictionConfig evictionConfig() {
        return new EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setComparatorClassName("comparatorClassName")
                .setSize(10)
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);
    }

    private static AwsConfig getDummyAwsConfig() {
        return new AwsConfig().setProperty("host-header", "dummyHost")
                .setProperty("region", "dummyRegion")
                .setEnabled(true)
                .setProperty("connection-timeout-seconds", "1")
                .setProperty("access-key", "dummyKey")
                .setProperty("iam-role", "dummyIam")
                .setProperty("secret-key", "dummySecretKey")
                .setProperty("security-group-name", "dummyGroupName")
                .setProperty("tag-key", "dummyTagKey")
                .setProperty("tag-value", "dummyTagValue");
    }

    private static DiscoveryConfig getDummyDiscoveryConfig() {
        DiscoveryStrategyConfig strategyConfig = new DiscoveryStrategyConfig(new TestDiscoveryStrategyFactory());
        strategyConfig.addProperty("prop1", "val1");
        strategyConfig.addProperty("prop2", "val2");

        DiscoveryConfig discoveryConfig = new DiscoveryConfig();
        discoveryConfig.setNodeFilter(candidate -> false);
        assert discoveryConfig.getNodeFilterClass() == null;
        assert discoveryConfig.getNodeFilter() != null;
        discoveryConfig.addDiscoveryStrategyConfig(strategyConfig);
        discoveryConfig.addDiscoveryStrategyConfig(new DiscoveryStrategyConfig("dummyClass2"));

        return discoveryConfig;
    }
}
