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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.cache.impl.event.CachePartitionLostEvent;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.CachePartitionLostListenerConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleEntryListenerConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.LockConfig;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.core.RingbufferStore;
import com.hazelcast.core.RingbufferStoreFactory;
import com.hazelcast.internal.eviction.EvictableEntryView;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.TopicOverloadPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.MultiMapConfig.ValueCollectionType.LIST;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DynamicConfigTest extends HazelcastTestSupport {

    protected static final int INSTANCE_COUNT = 2;
    protected static final String NON_DEFAULT_MERGE_POLICY = "AnotherMergePolicy";
    protected static final int NON_DEFAULT_MERGE_BATCH_SIZE = 31415;

    private String name = randomString();
    private HazelcastInstance[] members;
    // add***Config is invoked on driver instance
    private HazelcastInstance driver;

    @Before
    public void setup() {
        members = newInstances();
        driver = getDriver();
    }

    protected HazelcastInstance[] newInstances() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(INSTANCE_COUNT);
        return factory.newInstances();
    }

    protected HazelcastInstance getDriver() {
        return members[members.length - 1];
    }

    @Test
    public void testMultiMapConfig() {
        MultiMapConfig multiMapConfig = new MultiMapConfig(name)
                .setBackupCount(4)
                .setAsyncBackupCount(2)
                .setStatisticsEnabled(true)
                .setBinary(true)
                .setValueCollectionType(LIST)
                .addEntryListenerConfig(new EntryListenerConfig("com.hazelcast.Listener", true, false));

        driver.getConfig().addMultiMapConfig(multiMapConfig);

        assertConfigurationsEqualsOnAllMembers(multiMapConfig);
    }

    @Test
    public void testMultiMapConfig_whenEntryListenerConfigHasImplementation() {
        MultiMapConfig multiMapConfig = new MultiMapConfig(name)
                .setBackupCount(4)
                .setAsyncBackupCount(2)
                .setStatisticsEnabled(true)
                .setBinary(true)
                .setValueCollectionType(LIST)
                .setMergePolicyConfig(new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY, NON_DEFAULT_MERGE_BATCH_SIZE))
                .addEntryListenerConfig(new EntryListenerConfig(new SampleEntryListener(), true, false));

        driver.getConfig().addMultiMapConfig(multiMapConfig);

        assertConfigurationsEqualsOnAllMembers(multiMapConfig);
    }

    @Test
    public void testCardinalityEstimatorConfig() {
        CardinalityEstimatorConfig config = new CardinalityEstimatorConfig(name, 4, 2)
                .setMergePolicyConfig(new MergePolicyConfig("com.hazelcast.spi.merge.DiscardMergePolicy", 20));
        driver.getConfig().addCardinalityEstimatorConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testLockConfig() {
        LockConfig config = new LockConfig(name);
        config.setQuorumName(randomString());

        driver.getConfig().addLockConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testListConfig() {
        ListConfig config = getListConfig();

        driver.getConfig().addListConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testListConfig_withItemListenerConfigs() {
        ListConfig config = getListConfig();
        config.addItemListenerConfig(getItemListenerConfig_byClassName());
        config.addItemListenerConfig(getItemListenerConfig_byImplementation());

        driver.getConfig().addListConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testExecutorConfig() {
        ExecutorConfig config = new ExecutorConfig(name, 7)
                .setStatisticsEnabled(true)
                .setQueueCapacity(13);

        driver.getConfig().addExecutorConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testDurableExecutorConfig() {
        DurableExecutorConfig config = new DurableExecutorConfig(name, 7, 3, 10);

        driver.getConfig().addDurableExecutorConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testScheduledExecutorConfig() {
        ScheduledExecutorConfig config = new ScheduledExecutorConfig(name, 2, 3, 10, null,
                new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY, NON_DEFAULT_MERGE_BATCH_SIZE));

        driver.getConfig().addScheduledExecutorConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testRingbufferConfig() {
        RingbufferConfig config = getRingbufferConfig();

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byClassName() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig()
                .setEnabled(true)
                .setClassName("com.hazelcast.Foo");

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byFactoryClassName() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig()
                .setEnabled(true)
                .setFactoryClassName("com.hazelcast.FactoryFoo");

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byStoreImplementation() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig()
                .setEnabled(true)
                .setStoreImplementation(new SampleRingbufferStore());

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byFactoryImplementation() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig()
                .setEnabled(true)
                .setFactoryImplementation(new SampleRingbufferStoreFactory());

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testQueueConfig() {
        QueueConfig config = getQueueConfig();

        driver.getConfig().addQueueConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testQueueConfig_withListeners() {
        QueueConfig config = getQueueConfig_withListeners();

        driver.getConfig().addQueueConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testReplicatedMapDefaultConfig() {
        ReplicatedMapConfig config = new ReplicatedMapConfig(name);

        driver.getConfig().addReplicatedMapConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testReplicatedMapConfig_withNonDefaultMergePolicy() {
        ReplicatedMapConfig config = new ReplicatedMapConfig(name)
                .setMergePolicyConfig(new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY, NON_DEFAULT_MERGE_BATCH_SIZE));

        driver.getConfig().addReplicatedMapConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testSameReplicatedMapConfig_canBeAddedTwice() {
        ReplicatedMapConfig config = new ReplicatedMapConfig(name);
        driver.getConfig().addReplicatedMapConfig(config);
        ReplicatedMap map = driver.getReplicatedMap(name);
        driver.getConfig().addReplicatedMapConfig(config);
        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testReplicatedMapConfig_withListenerByClassName() {
        ReplicatedMapConfig config = new ReplicatedMapConfig(name)
                .setStatisticsEnabled(true)
                .setMergePolicy("com.hazelcast.SomeMergePolicy")
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .addEntryListenerConfig(new EntryListenerConfig(randomString(), true, false));
        config.setAsyncFillup(true);

        driver.getConfig().addReplicatedMapConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testReplicatedMapConfig_withListenerByImplementation() {
        ReplicatedMapConfig config = new ReplicatedMapConfig(name)
                .setStatisticsEnabled(true)
                .setMergePolicy("com.hazelcast.SomeMergePolicy")
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .addEntryListenerConfig(new EntryListenerConfig(new SampleEntryListener(), false, true));
        config.setAsyncFillup(true);

        driver.getConfig().addReplicatedMapConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testSetConfig() {
        SetConfig setConfig = getSetConfig(name);

        driver.getConfig().addSetConfig(setConfig);

        assertConfigurationsEqualsOnAllMembers(setConfig);
    }

    @Test
    public void testMapConfig() {
        MapConfig config = getMapConfig();

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testDefaultMapConfig() {
        MapConfig config = new MapConfig(name);

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withEntryListenerImplementation() {
        MapConfig config = getMapConfig_withEntryListenerImplementation();

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withEntryListenerClassName() {
        MapConfig config = getMapConfig_withEntryListenerClassName();

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withQueryCacheConfig() {
        MapConfig config = getMapConfig_withQueryCacheConfig();

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withQueryCacheConfig_andEntryListenerConfigByClassName() {
        MapConfig config = getMapConfig_withQueryCacheConfig();
        config.getQueryCacheConfigs().get(0).addEntryListenerConfig(entryListenerConfigWithClassName());

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withQueryCacheConfig_andEntryListenerConfigByImplementation() {
        MapConfig config = getMapConfig_withQueryCacheConfig();
        config.getQueryCacheConfigs().get(0).addEntryListenerConfig(entryListenerConfigWithImplementation());

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withMapPartitionLostListener_byClassName() {
        MapConfig config = getMapConfig();
        config.addMapPartitionLostListenerConfig(getMapPartitionLostListenerConfig_byClassName());

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withMapPartitionLostListener_byImplementation() {
        MapConfig config = getMapConfig()
                .addMapPartitionLostListenerConfig(getMapPartitionLostListenerConfig_byImplementation());

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testSetConfig_whenItemListenersConfigured() {
        SetConfig setConfig = getSetConfig(name);
        setConfig.addItemListenerConfig(getItemListenerConfig_byImplementation());
        setConfig.addItemListenerConfig(getItemListenerConfig_byClassName());

        driver.getConfig().addSetConfig(setConfig);

        assertConfigurationsEqualsOnAllMembers(setConfig);
    }

    @Test
    public void testDefaultCacheConfig() {
        CacheSimpleConfig config = new CacheSimpleConfig()
                .setName(name);

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testCacheConfig() {
        CacheSimpleConfig config = getCacheConfig()
                .setExpiryPolicyFactoryConfig(new ExpiryPolicyFactoryConfig("expiryPolicyFactory"));

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withEvictionPolicy_cacheLoaderAndWriter() {
        CacheSimpleConfig config = getCacheConfig()
                .setEvictionConfig(getEvictionConfigByPolicy())
                .setCacheLoader("com.hazelcast.CacheLoader")
                .setCacheWriter("com.hazelcast.CacheWriter")
                // also exercise alternative method to set expiry policy factory config
                .setExpiryPolicyFactory("expiryPolicyFactory");

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withEvictionPolicyImplementation_cacheLoaderAndWriterFactory() {
        CacheSimpleConfig config = getCacheConfig()
                .setEvictionConfig(getEvictionConfigByImplementation())
                .setCacheLoaderFactory("com.hazelcast.CacheLoaderFactory")
                .setCacheWriterFactory("com.hazelcast.CacheWriterFactory");

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withTimedExpiryPolicyFactory() {
        CacheSimpleConfig config = getCacheConfig()
                .setExpiryPolicyFactoryConfig(new ExpiryPolicyFactoryConfig(
                        new TimedExpiryPolicyFactoryConfig(TimedExpiryPolicyFactoryConfig.ExpiryPolicyType.TOUCHED,
                                new ExpiryPolicyFactoryConfig.DurationConfig(130, TimeUnit.SECONDS))
                ));

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withPartitionLostListenerByClassName() {
        CacheSimpleConfig config = getCacheConfig()
                .addCachePartitionLostListenerConfig(new CachePartitionLostListenerConfig("partitionLostListener"));

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withPartitionLostListenerByImplementation() {
        CacheSimpleConfig config = getCacheConfig()
                .addCachePartitionLostListenerConfig(new CachePartitionLostListenerConfig(
                        new SampleCachePartitionLostListener()
                ));

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testEventJournalConfig() {
        EventJournalConfig cacheJournalConfig = new EventJournalConfig()
                .setEnabled(true)
                .setCacheName(randomName())
                .setCapacity(39)
                .setTimeToLiveSeconds(98);
        EventJournalConfig mapJournalConfig = new EventJournalConfig()
                .setEnabled(true)
                .setMapName(randomName())
                .setCapacity(42)
                .setTimeToLiveSeconds(52);

        driver.getConfig().addEventJournalConfig(cacheJournalConfig);
        driver.getConfig().addEventJournalConfig(mapJournalConfig);

        assertConfigurationsEqualsOnAllMembers(mapJournalConfig, cacheJournalConfig);
    }

    @Test
    public void testFlakeIdGeneratorConfig() {
        FlakeIdGeneratorConfig config = new FlakeIdGeneratorConfig(randomName())
                .setPrefetchCount(123)
                .setPrefetchValidityMillis(456)
                .setIdOffset(789)
                .setNodeIdOffset(890)
                .setStatisticsEnabled(false);
        driver.getConfig().addFlakeIdGeneratorConfig(config);
        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testSemaphoreConfig() {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig()
                .setName(name)
                .setInitialPermits(33)
                .setAsyncBackupCount(4)
                .setBackupCount(2);

        driver.getConfig().addSemaphoreConfig(semaphoreConfig);

        assertConfigurationsEqualsOnAllMembers(semaphoreConfig);
    }

    @Test
    public void testTopicConfig() {
        TopicConfig topicConfig = new TopicConfig(name)
                .setGlobalOrderingEnabled(true)
                .setMultiThreadingEnabled(false)
                .setStatisticsEnabled(true);

        driver.getConfig().addTopicConfig(topicConfig);

        assertConfigurationsEqualsOnAllMembers(topicConfig);
    }

    @Test
    public void testTopicConfig_whenListenerConfigByClassName() {
        TopicConfig topicConfig = new TopicConfig(name)
                .setGlobalOrderingEnabled(false)
                .setMultiThreadingEnabled(true)
                .setStatisticsEnabled(true)
                .addMessageListenerConfig(getListenerConfig_byClassName());

        driver.getConfig().addTopicConfig(topicConfig);

        assertConfigurationsEqualsOnAllMembers(topicConfig);
    }

    @Test
    public void testTopicConfig_whenListenerConfigByImplementation() {
        TopicConfig topicConfig = new TopicConfig(name)
                .setGlobalOrderingEnabled(false)
                .setMultiThreadingEnabled(true)
                .setStatisticsEnabled(true)
                .addMessageListenerConfig(getMessageListenerConfig_byImplementation());

        driver.getConfig().addTopicConfig(topicConfig);

        assertConfigurationsEqualsOnAllMembers(topicConfig);
    }

    @Test
    public void testReliableTopicConfig() {
        ReliableTopicConfig reliableTopicConfig = new ReliableTopicConfig(name)
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_OLDEST)
                .setReadBatchSize(42)
                .setStatisticsEnabled(true);

        driver.getConfig().addReliableTopicConfig(reliableTopicConfig);

        assertConfigurationsEqualsOnAllMembers(reliableTopicConfig);
    }

    @Test
    public void testReliableTopicConfig_whenListenerConfigByClassName() {
        ReliableTopicConfig reliableTopicConfig = new ReliableTopicConfig(name)
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_OLDEST)
                .setReadBatchSize(42)
                .setStatisticsEnabled(true)
                .addMessageListenerConfig(getListenerConfig_byClassName());

        driver.getConfig().addReliableTopicConfig(reliableTopicConfig);

        assertConfigurationsEqualsOnAllMembers(reliableTopicConfig);
    }

    @Test
    public void testReliableTopicConfig_whenListenerConfigByImplementation() {
        ReliableTopicConfig reliableTopicConfig = new ReliableTopicConfig(name)
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_OLDEST)
                .setReadBatchSize(42)
                .setStatisticsEnabled(true)
                .addMessageListenerConfig(getMessageListenerConfig_byImplementation());

        driver.getConfig().addReliableTopicConfig(reliableTopicConfig);

        assertConfigurationsEqualsOnAllMembers(reliableTopicConfig);
    }

    @Test
    public void testReliableTopicConfig_whenHasExecutor() {
        ReliableTopicConfig reliableTopicConfig = new ReliableTopicConfig(name)
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_OLDEST)
                .setReadBatchSize(42)
                .setStatisticsEnabled(true)
                .setExecutor(new SampleExecutor());

        driver.getConfig().addReliableTopicConfig(reliableTopicConfig);

        assertConfigurationsEqualsOnAllMembers(reliableTopicConfig);
    }

    private void assertConfigurationsEqualsOnAllMembers(CacheSimpleConfig config) {
        String name = config.getName();
        for (HazelcastInstance instance : members) {
            CacheSimpleConfig registeredConfig = instance.getConfig().getCacheConfig(name);
            assertEquals(config, registeredConfig);
        }
    }

    private void assertConfigurationsEqualsOnAllMembers(QueueConfig queueConfig) {
        String name = queueConfig.getName();
        for (HazelcastInstance instance : members) {
            QueueConfig registeredConfig = instance.getConfig().getQueueConfig(name);
            assertEquals(queueConfig, registeredConfig);
        }
    }

    private void assertConfigurationsEqualsOnAllMembers(LockConfig lockConfig) {
        String name = lockConfig.getName();
        for (HazelcastInstance instance : members) {
            LockConfig registeredConfig = instance.getConfig().getLockConfig(name);
            assertEquals(lockConfig, registeredConfig);
        }
    }

    private void assertConfigurationsEqualsOnAllMembers(CardinalityEstimatorConfig cardinalityEstimatorConfig) {
        String name = cardinalityEstimatorConfig.getName();
        for (HazelcastInstance instance : members) {
            CardinalityEstimatorConfig registeredConfig = instance.getConfig().getCardinalityEstimatorConfig(name);
            assertEquals(cardinalityEstimatorConfig, registeredConfig);
        }
    }

    private void assertConfigurationsEqualsOnAllMembers(MultiMapConfig multiMapConfig) {
        String name = multiMapConfig.getName();
        for (HazelcastInstance instance : members) {
            MultiMapConfig registeredConfig = instance.getConfig().getMultiMapConfig(name);
            assertEquals(multiMapConfig, registeredConfig);
        }
    }

    private void assertConfigurationsEqualsOnAllMembers(ExecutorConfig executorConfig) {
        String name = executorConfig.getName();
        for (HazelcastInstance instance : members) {
            ExecutorConfig registeredConfig = instance.getConfig().getExecutorConfig(name);
            assertEquals(executorConfig, registeredConfig);
        }
    }

    private void assertConfigurationsEqualsOnAllMembers(RingbufferConfig ringbufferConfig) {
        String name = ringbufferConfig.getName();
        for (HazelcastInstance instance : members) {
            RingbufferConfig registeredConfig = instance.getConfig().getRingbufferConfig(name);
            assertEquals(ringbufferConfig, registeredConfig);
        }
    }

    private void assertConfigurationsEqualsOnAllMembers(DurableExecutorConfig durableExecutorConfig) {
        String name = durableExecutorConfig.getName();
        for (HazelcastInstance instance : members) {
            DurableExecutorConfig registeredConfig = instance.getConfig().getDurableExecutorConfig(name);
            assertEquals(durableExecutorConfig, registeredConfig);
        }
    }

    private void assertConfigurationsEqualsOnAllMembers(ScheduledExecutorConfig scheduledExecutorConfig) {
        String name = scheduledExecutorConfig.getName();
        for (HazelcastInstance instance : members) {
            ScheduledExecutorConfig registeredConfig = instance.getConfig().getScheduledExecutorConfig(name);
            assertEquals(scheduledExecutorConfig, registeredConfig);
        }
    }

    private void assertConfigurationsEqualsOnAllMembers(SetConfig setConfig) {
        String name = setConfig.getName();
        for (HazelcastInstance instance : members) {
            SetConfig registeredConfig = instance.getConfig().getSetConfig(name);
            assertEquals(setConfig, registeredConfig);
        }
    }

    private void assertConfigurationsEqualsOnAllMembers(MapConfig mapConfig) {
        String name = mapConfig.getName();
        for (HazelcastInstance instance : members) {
            MapConfig registeredConfig = instance.getConfig().getMapConfig(name);
            assertEquals(mapConfig, registeredConfig);
        }
    }

    private void assertConfigurationsEqualsOnAllMembers(ReplicatedMapConfig replicatedMapConfig) {
        String name = replicatedMapConfig.getName();
        for (HazelcastInstance instance : members) {
            ReplicatedMapConfig registeredConfig = instance.getConfig().getReplicatedMapConfig(name);
            assertEquals(replicatedMapConfig, registeredConfig);
        }
    }

    private void assertConfigurationsEqualsOnAllMembers(ListConfig listConfig) {
        String name = listConfig.getName();
        for (HazelcastInstance instance : members) {
            ListConfig registeredConfig = instance.getConfig().getListConfig(name);
            assertEquals(listConfig, registeredConfig);
        }
    }

    private void assertConfigurationsEqualsOnAllMembers(EventJournalConfig mapEventJournalConfig,
                                                        EventJournalConfig cacheEventJournalConfig) {
        String cacheName = cacheEventJournalConfig.getCacheName();
        String mapName = mapEventJournalConfig.getMapName();
        for (HazelcastInstance instance : members) {
            EventJournalConfig registeredConfig = instance.getConfig().getCacheEventJournalConfig(cacheName);
            assertEquals(cacheEventJournalConfig, registeredConfig);
            registeredConfig = instance.getConfig().getMapEventJournalConfig(mapName);
            assertEquals(mapEventJournalConfig, registeredConfig);
        }
    }

    private void assertConfigurationsEqualsOnAllMembers(FlakeIdGeneratorConfig config) {
        for (HazelcastInstance instance : members) {
            FlakeIdGeneratorConfig registeredConfig = instance.getConfig().getFlakeIdGeneratorConfig(config.getName());
            assertEquals(config, registeredConfig);
        }
    }

    private void assertConfigurationsEqualsOnAllMembers(TopicConfig topicConfig) {
        String name = topicConfig.getName();
        for (HazelcastInstance instance : members) {
            TopicConfig registeredConfig = instance.getConfig().getTopicConfig(name);
            assertEquals(topicConfig, registeredConfig);
        }
    }

    private void assertConfigurationsEqualsOnAllMembers(ReliableTopicConfig reliableTopicConfig) {
        String name = reliableTopicConfig.getName();
        for (HazelcastInstance instance : members) {
            ReliableTopicConfig registeredConfig = instance.getConfig().getReliableTopicConfig(name);
            assertEquals(reliableTopicConfig, registeredConfig);
        }
    }

    private void assertConfigurationsEqualsOnAllMembers(SemaphoreConfig semaphoreConfig) {
        String name = semaphoreConfig.getName();
        for (HazelcastInstance instance : members) {
            SemaphoreConfig registeredConfig = instance.getConfig().getSemaphoreConfig(name);
            assertEquals(semaphoreConfig, registeredConfig);
        }
    }

    private CacheSimpleConfig getCacheConfig() {
        CacheSimpleEntryListenerConfig entryListenerConfig = new CacheSimpleEntryListenerConfig();
        entryListenerConfig.setCacheEntryListenerFactory("CacheEntryListenerFactory");
        entryListenerConfig.setSynchronous(true);
        entryListenerConfig.setOldValueRequired(true);
        entryListenerConfig.setCacheEntryEventFilterFactory("CacheEntryEventFilterFactory");

        CacheSimpleConfig config = new CacheSimpleConfig()
                .setName(name)
                .setQuorumName("quorum")
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setBackupCount(3)
                .setAsyncBackupCount(2)
                .addEntryListenerConfig(entryListenerConfig)
                .setStatisticsEnabled(true)
                .setManagementEnabled(true)
                .setKeyType("keyType")
                .setValueType("valueType")
                .setReadThrough(true)
                .setWriteThrough(true)
                .setHotRestartConfig(new HotRestartConfig().setEnabled(true).setFsync(true));

        config.setWanReplicationRef(new WanReplicationRef(randomName(), "com.hazelcast.MergePolicy",
                Collections.singletonList("filter"), true));
        config.setMergePolicy("mergePolicy");
        config.setDisablePerEntryInvalidationEvents(true);
        return config;
    }

    private EvictionConfig getEvictionConfigByPolicy() {
        return new EvictionConfig(39, EvictionConfig.MaxSizePolicy.ENTRY_COUNT, EvictionPolicy.RANDOM);
    }

    private EvictionConfig getEvictionConfigByClassName() {
        return new EvictionConfig(39, EvictionConfig.MaxSizePolicy.ENTRY_COUNT, "com.hazelcast.Comparator");
    }

    private EvictionConfig getEvictionConfigByImplementation() {
        return new EvictionConfig(39, EvictionConfig.MaxSizePolicy.ENTRY_COUNT, new SampleEvictionPolicyComparator());
    }

    private SetConfig getSetConfig(String name) {
        return new SetConfig(name)
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setMaxSize(99)
                .setStatisticsEnabled(true)
                .setMergePolicyConfig(new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY, NON_DEFAULT_MERGE_BATCH_SIZE));
    }

    private MapConfig getMapConfig() {
        return new MapConfig(name)
                .setAsyncBackupCount(3)
                .setBackupCount(2)
                .setCacheDeserializedValues(CacheDeserializedValues.ALWAYS)
                .setEvictionPolicy(EvictionPolicy.RANDOM)
                .setHotRestartConfig(new HotRestartConfig().setEnabled(true).setFsync(true))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setMergePolicyConfig(new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY, NON_DEFAULT_MERGE_BATCH_SIZE))
                .setMaxSizeConfig(new MaxSizeConfig(4096, MaxSizeConfig.MaxSizePolicy.PER_NODE))
                .setTimeToLiveSeconds(220)
                .setMaxIdleSeconds(110)
                .setQuorumName(randomString())
                .addMapAttributeConfig(new MapAttributeConfig("attributeName", "com.attribute.extractor"))
                .addMapIndexConfig(new MapIndexConfig("attr", true));
    }

    private MapConfig getMapConfig_withEntryListenerImplementation() {
        return new MapConfig(name).addEntryListenerConfig(entryListenerConfigWithImplementation());
    }

    private MapConfig getMapConfig_withEntryListenerClassName() {
        return new MapConfig(name).addEntryListenerConfig(entryListenerConfigWithClassName());
    }

    private MapConfig getMapConfig_withQueryCacheConfig() {
        return new MapConfig(name).addQueryCacheConfig(getQueryCacheConfig());
    }

    private EntryListenerConfig entryListenerConfigWithImplementation() {
        return new EntryListenerConfig(new SampleEntryListener(), false, true);
    }

    private EntryListenerConfig entryListenerConfigWithClassName() {
        return new EntryListenerConfig("com.hazelcast.someListener", false, true);
    }

    private QueryCacheConfig getQueryCacheConfig() {
        return new QueryCacheConfig(randomName())
                .setBatchSize(131)
                .setDelaySeconds(98)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setBufferSize(873)
                .setPopulate(true)
                .setIncludeValue(true)
                .setCoalesce(true)
                .setPredicateConfig(new PredicateConfig("com.hazelcast.Predicate"))
                .setEvictionConfig(new EvictionConfig(32, EvictionConfig.MaxSizePolicy.ENTRY_COUNT, "com.hazelcast.Comparator"));
    }

    private MapPartitionLostListenerConfig getMapPartitionLostListenerConfig_byClassName() {
        return new MapPartitionLostListenerConfig("com.hazelcast.PartitionLostListener");
    }

    private MapPartitionLostListenerConfig getMapPartitionLostListenerConfig_byImplementation() {
        return new MapPartitionLostListenerConfig(new SamplePartitionLostListener());
    }

    private ListConfig getListConfig() {
        return new ListConfig(name)
                .setStatisticsEnabled(true)
                .setMaxSize(99)
                .setBackupCount(4)
                .setAsyncBackupCount(2)
                .setMergePolicyConfig(new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY, NON_DEFAULT_MERGE_BATCH_SIZE));
    }

    private RingbufferConfig getRingbufferConfig() {
        return new RingbufferConfig(name)
                .setTimeToLiveSeconds(59)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setCapacity(33)
                .setBackupCount(4)
                .setAsyncBackupCount(2)
                .setMergePolicyConfig(new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY, NON_DEFAULT_MERGE_BATCH_SIZE));
    }

    public QueueConfig getQueueConfig() {
        String name = randomName();
        return new QueueConfig(name)
                .setBackupCount(2)
                .setAsyncBackupCount(2)
                // no explicit max size - let's test encoding of the default value
                .setEmptyQueueTtl(10)
                .setQueueStoreConfig(new QueueStoreConfig().setClassName("foo.bar.ImplName").setEnabled(true))
                .setStatisticsEnabled(false)
                .setQuorumName("myQuorum")
                .setMergePolicyConfig(new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY, NON_DEFAULT_MERGE_BATCH_SIZE));
    }

    public QueueConfig getQueueConfig_withListeners() {
        String name = randomName();
        return new QueueConfig(name)
                .addItemListenerConfig(getItemListenerConfig_byClassName())
                .addItemListenerConfig(getItemListenerConfig_byImplementation())
                .setBackupCount(2)
                .setAsyncBackupCount(2)
                .setMaxSize(1000)
                .setEmptyQueueTtl(10)
                .setQueueStoreConfig(new QueueStoreConfig().setClassName("foo.bar.ImplName").setEnabled(true))
                .setStatisticsEnabled(false)
                .setQuorumName("myQuorum");
    }

    private ListenerConfig getListenerConfig_byClassName() {
        return new ListenerConfig("com.hazelcast.ListenerClassName");
    }

    private ListenerConfig getMessageListenerConfig_byImplementation() {
        return new ListenerConfig(new SampleMessageListener());
    }

    private ItemListenerConfig getItemListenerConfig_byClassName() {
        return new ItemListenerConfig("com.hazelcast.ItemListener", true);
    }

    private ItemListenerConfig getItemListenerConfig_byImplementation() {
        return new ItemListenerConfig(new SampleItemListener(), true);
    }

    public static class SampleEntryListener implements EntryAddedListener, Serializable {

        @Override
        public void entryAdded(EntryEvent event) {
        }

        @Override
        public int hashCode() {
            return 31;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof SampleEntryListener;
        }
    }

    public static class SampleItemListener implements ItemListener, Serializable {

        @Override
        public void itemAdded(ItemEvent item) {
        }

        @Override
        public void itemRemoved(ItemEvent item) {
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleItemListener);
        }

        @Override
        public int hashCode() {
            return 33;
        }
    }

    public static class SampleRingbufferStore implements RingbufferStore, Serializable {

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

        @Override
        public int hashCode() {
            return 33;
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleRingbufferStore);
        }
    }

    public static class SampleRingbufferStoreFactory implements RingbufferStoreFactory, Serializable {

        @Override
        public RingbufferStore newRingbufferStore(String name, Properties properties) {
            return null;
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleRingbufferStoreFactory);
        }
    }

    public static class SamplePartitionLostListener implements MapPartitionLostListener, Serializable {

        @Override
        public void partitionLost(MapPartitionLostEvent event) {
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SamplePartitionLostListener);
        }
    }

    public static class SampleEvictionPolicyComparator extends EvictionPolicyComparator {

        @Override
        public int compare(EvictableEntryView e1, EvictableEntryView e2) {
            return 0;
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleEvictionPolicyComparator);
        }
    }

    public static class SampleCachePartitionLostListener implements CachePartitionLostListener, Serializable {

        @Override
        public void partitionLost(CachePartitionLostEvent event) {
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleCachePartitionLostListener);
        }
    }

    public static class SampleMessageListener implements MessageListener, Serializable {

        @Override
        public void onMessage(Message message) {
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleMessageListener);
        }
    }

    public static class SampleExecutor implements Executor, Serializable {

        @Override
        public void execute(Runnable command) {
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleExecutor);
        }
    }
}
