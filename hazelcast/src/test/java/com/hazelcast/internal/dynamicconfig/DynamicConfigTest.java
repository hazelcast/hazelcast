/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.CachePartitionLostListenerConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleEntryListenerConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.diagnostics.DiagnosticsConfig;
import com.hazelcast.internal.diagnostics.DiagnosticsOutputType;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.config.MetadataPolicy;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NamedConfig;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.ringbuffer.RingbufferStore;
import com.hazelcast.ringbuffer.RingbufferStoreFactory;
import com.hazelcast.spi.eviction.EvictableEntryView;
import com.hazelcast.spi.eviction.EvictionPolicyComparator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import com.hazelcast.topic.TopicOverloadPolicy;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.MultiMapConfig.ValueCollectionType.LIST;
import static com.hazelcast.dataconnection.impl.DataConnectionTestUtil.DUMMY_TYPE;
import static com.hazelcast.test.TestConfigUtils.NON_DEFAULT_BACKUP_COUNT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigTest extends HazelcastTestSupport {

    protected static final int INSTANCE_COUNT = 2;
    protected static final String NON_DEFAULT_MERGE_POLICY = "AnotherMergePolicy";
    protected static final int NON_DEFAULT_MERGE_BATCH_SIZE = 31415;

    private final String name = randomString();
    private HazelcastInstance[] members;
    // add***Config is invoked on driver instance
    protected HazelcastInstance driver;

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
    public void testAddWanReplicationConfigIsNotSupported() {
        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName(name);

        UnsupportedOperationException exception = Assertions.catchThrowableOfType(
                () -> getDriver().getConfig().addWanReplicationConfig(wanReplicationConfig),
                UnsupportedOperationException.class);
        assertNotNull(exception);
        assertThat(exception).hasMessage("Adding new WAN config is not supported.");
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

        assertConfigurationsEqualOnAllMembers(multiMapConfig);
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

        assertConfigurationsEqualOnAllMembers(multiMapConfig);
    }

    @Test
    public void testCardinalityEstimatorConfig() {
        CardinalityEstimatorConfig config = new CardinalityEstimatorConfig(name, 4, 2)
                .setMergePolicyConfig(new MergePolicyConfig("com.hazelcast.spi.merge.DiscardMergePolicy", 20));
        driver.getConfig().addCardinalityEstimatorConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testListConfig() {
        ListConfig config = getListConfig();

        driver.getConfig().addListConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testListConfig_withItemListenerConfigs() {
        ListConfig config = getListConfig();
        config.addItemListenerConfig(getItemListenerConfig_byClassName());
        config.addItemListenerConfig(getItemListenerConfig_byImplementation());

        driver.getConfig().addListConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testExecutorConfig() {
        ExecutorConfig config = new ExecutorConfig(name, 7)
                .setStatisticsEnabled(true)
                .setQueueCapacity(13);

        driver.getConfig().addExecutorConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testDurableExecutorConfig() {
        DurableExecutorConfig config = new DurableExecutorConfig(name, 7, 3, 10, false, null);

        driver.getConfig().addDurableExecutorConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testScheduledExecutorConfig() {
        ScheduledExecutorConfig config = new ScheduledExecutorConfig(name, 2, 3, 10, null,
                new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY, NON_DEFAULT_MERGE_BATCH_SIZE),
                ScheduledExecutorConfig.CapacityPolicy.PER_NODE, false, null);

        driver.getConfig().addScheduledExecutorConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testRingbufferConfig() {
        RingbufferConfig config = getRingbufferConfig();

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byClassName() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig()
                .setEnabled(true)
                .setClassName("com.hazelcast.Foo");

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byFactoryClassName() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig()
                .setEnabled(true)
                .setFactoryClassName("com.hazelcast.FactoryFoo");

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byStoreImplementation() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig()
                .setEnabled(true)
                .setStoreImplementation(new SampleRingbufferStore());

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byFactoryImplementation() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig()
                .setEnabled(true)
                .setFactoryImplementation(new SampleRingbufferStoreFactory());

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testQueueConfig() {
        QueueConfig config = getQueueConfig();

        driver.getConfig().addQueueConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testQueueConfig_withListeners() {
        QueueConfig config = getQueueConfig_withListeners();

        driver.getConfig().addQueueConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testReplicatedMapDefaultConfig() {
        ReplicatedMapConfig config = new ReplicatedMapConfig(name);

        driver.getConfig().addReplicatedMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testReplicatedMapConfig_withNonDefaultMergePolicy() {
        ReplicatedMapConfig config = new ReplicatedMapConfig(name)
                .setMergePolicyConfig(new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY, NON_DEFAULT_MERGE_BATCH_SIZE));

        driver.getConfig().addReplicatedMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testSameReplicatedMapConfig_canBeAddedTwice() {
        ReplicatedMapConfig config = new ReplicatedMapConfig(name);
        driver.getConfig().addReplicatedMapConfig(config);
        ReplicatedMap map = driver.getReplicatedMap(name);
        driver.getConfig().addReplicatedMapConfig(config);
        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testReplicatedMapConfig_withListenerByClassName() {
        ReplicatedMapConfig config = new ReplicatedMapConfig(name)
                .setStatisticsEnabled(true)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .addEntryListenerConfig(new EntryListenerConfig(randomString(), true, false));
        config.getMergePolicyConfig().setPolicy("com.hazelcast.SomeMergePolicy");
        config.setAsyncFillup(true);

        driver.getConfig().addReplicatedMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testReplicatedMapConfig_withListenerByImplementation() {
        ReplicatedMapConfig config = new ReplicatedMapConfig(name)
                .setStatisticsEnabled(true)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .addEntryListenerConfig(new EntryListenerConfig(new SampleEntryListener(), false, true));
        config.getMergePolicyConfig().setPolicy("com.hazelcast.SomeMergePolicy");
        config.setAsyncFillup(true);

        driver.getConfig().addReplicatedMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testSetConfig() {
        SetConfig setConfig = getSetConfig(name);

        driver.getConfig().addSetConfig(setConfig);

        assertConfigurationsEqualOnAllMembers(setConfig);
    }

    @Test
    public void testMapConfig() {
        MapConfig config = getMapConfig();

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testDefaultMapConfig() {
        MapConfig config = new MapConfig(name);

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withEntryListenerImplementation() {
        MapConfig config = getMapConfig_withEntryListenerImplementation();

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withEntryListenerClassName() {
        MapConfig config = getMapConfig_withEntryListenerClassName();

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withQueryCacheConfig() {
        MapConfig config = getMapConfig_withQueryCacheConfig();

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withQueryCacheConfig_andEntryListenerConfigByClassName() {
        MapConfig config = getMapConfig_withQueryCacheConfig();
        config.getQueryCacheConfigs().get(0).addEntryListenerConfig(entryListenerConfigWithClassName());

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withQueryCacheConfig_andEntryListenerConfigByImplementation() {
        MapConfig config = getMapConfig_withQueryCacheConfig();
        config.getQueryCacheConfigs().get(0).addEntryListenerConfig(entryListenerConfigWithImplementation());

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withMapPartitionLostListener_byClassName() {
        MapConfig config = getMapConfig();
        config.addMapPartitionLostListenerConfig(getMapPartitionLostListenerConfig_byClassName());

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withMapPartitionLostListener_byImplementation() {
        MapConfig config = getMapConfig()
                .addMapPartitionLostListenerConfig(getMapPartitionLostListenerConfig_byImplementation());

        driver.getConfig().addMapConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testMapConfig_withDifferentOrderOfIndexes() {
        MapConfig config = new MapConfig(name);
        IndexConfig idx1 = new IndexConfig(IndexType.SORTED, "foo");
        idx1.setName("idx1");
        IndexConfig idx2 = new IndexConfig(IndexType.SORTED, "bar");
        idx2.setName("idx2");
        config.setIndexConfigs(List.of(idx1, idx2));

        MapConfig reordered = new MapConfig(name);
        reordered.setIndexConfigs(List.of(idx2, idx1));

        driver.getConfig().addMapConfig(config);
        assertThatNoException().isThrownBy(() -> driver.getConfig().addMapConfig(reordered));

        assertConfigurationsEqualOnAllMembers(config);
        assertConfigurationsEqualOnAllMembers(reordered);
    }

    @Test
    public void testSetConfig_whenItemListenersConfigured() {
        SetConfig setConfig = getSetConfig(name);
        setConfig.addItemListenerConfig(getItemListenerConfig_byImplementation());
        setConfig.addItemListenerConfig(getItemListenerConfig_byClassName());

        driver.getConfig().addSetConfig(setConfig);

        assertConfigurationsEqualOnAllMembers(setConfig);
    }

    @Test
    public void testDefaultCacheConfig() {
        CacheSimpleConfig config = new CacheSimpleConfig()
                .setName(name);

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testSameCacheConfig_canBeAddedTwice() {
        CacheSimpleConfig config = new CacheSimpleConfig()
                .setName(name)
                .setBackupCount(NON_DEFAULT_BACKUP_COUNT);

        driver.getConfig().addCacheConfig(config);
        driver.getCacheManager().getCache(name);
        driver.getConfig().addCacheConfig(config);
    }

    @Test
    public void testCacheConfig() {
        CacheSimpleConfig config = getCacheConfig()
                .setExpiryPolicyFactoryConfig(new ExpiryPolicyFactoryConfig("expiryPolicyFactory"));

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
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

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withEvictionPolicyImplementation_cacheLoaderAndWriterFactory() {
        CacheSimpleConfig config = getCacheConfig()
                .setEvictionConfig(getEvictionConfigByImplementation())
                .setCacheLoaderFactory("com.hazelcast.CacheLoaderFactory")
                .setCacheWriterFactory("com.hazelcast.CacheWriterFactory");

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withTimedExpiryPolicyFactory() {
        CacheSimpleConfig config = getCacheConfig()
                .setExpiryPolicyFactoryConfig(new ExpiryPolicyFactoryConfig(
                        new TimedExpiryPolicyFactoryConfig(TimedExpiryPolicyFactoryConfig.ExpiryPolicyType.TOUCHED,
                                new ExpiryPolicyFactoryConfig.DurationConfig(130, TimeUnit.SECONDS))
                ));

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withPartitionLostListenerByClassName() {
        CacheSimpleConfig config = getCacheConfig()
                .addCachePartitionLostListenerConfig(new CachePartitionLostListenerConfig("partitionLostListener"));

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withPartitionLostListenerByImplementation() {
        CacheSimpleConfig config = getCacheConfig()
                .addCachePartitionLostListenerConfig(new CachePartitionLostListenerConfig(
                        new SampleCachePartitionLostListener()
                ));

        driver.getConfig().addCacheConfig(config);

        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testFlakeIdGeneratorConfig() {
        FlakeIdGeneratorConfig config = new FlakeIdGeneratorConfig(randomName())
                .setPrefetchCount(123)
                .setPrefetchValidityMillis(456)
                .setEpochStart(789)
                .setNodeIdOffset(890)
                .setBitsNodeId(11)
                .setBitsSequence(22)
                .setStatisticsEnabled(false);
        driver.getConfig().addFlakeIdGeneratorConfig(config);
        assertConfigurationsEqualOnAllMembers(config);
    }

    @Test
    public void testTopicConfig() {
        TopicConfig topicConfig = new TopicConfig(name)
                .setGlobalOrderingEnabled(true)
                .setMultiThreadingEnabled(false)
                .setStatisticsEnabled(true);

        driver.getConfig().addTopicConfig(topicConfig);

        assertConfigurationsEqualOnAllMembers(topicConfig);
    }

    @Test
    public void testTopicConfig_whenListenerConfigByClassName() {
        TopicConfig topicConfig = new TopicConfig(name)
                .setGlobalOrderingEnabled(false)
                .setMultiThreadingEnabled(true)
                .setStatisticsEnabled(true)
                .addMessageListenerConfig(getListenerConfig_byClassName());

        driver.getConfig().addTopicConfig(topicConfig);

        assertConfigurationsEqualOnAllMembers(topicConfig);
    }

    @Test
    public void testTopicConfig_whenListenerConfigByImplementation() {
        TopicConfig topicConfig = new TopicConfig(name)
                .setGlobalOrderingEnabled(false)
                .setMultiThreadingEnabled(true)
                .setStatisticsEnabled(true)
                .addMessageListenerConfig(getMessageListenerConfig_byImplementation());

        driver.getConfig().addTopicConfig(topicConfig);

        assertConfigurationsEqualOnAllMembers(topicConfig);
    }

    @Test
    public void testReliableTopicConfig() {
        ReliableTopicConfig reliableTopicConfig = new ReliableTopicConfig(name)
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_OLDEST)
                .setReadBatchSize(42)
                .setStatisticsEnabled(true);

        driver.getConfig().addReliableTopicConfig(reliableTopicConfig);

        assertConfigurationsEqualOnAllMembers(reliableTopicConfig);
    }

    @Test
    public void testReliableTopicConfig_whenListenerConfigByClassName() {
        ReliableTopicConfig reliableTopicConfig = new ReliableTopicConfig(name)
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_OLDEST)
                .setReadBatchSize(42)
                .setStatisticsEnabled(true)
                .addMessageListenerConfig(getListenerConfig_byClassName());

        driver.getConfig().addReliableTopicConfig(reliableTopicConfig);

        assertConfigurationsEqualOnAllMembers(reliableTopicConfig);
    }

    @Test
    public void testReliableTopicConfig_whenListenerConfigByImplementation() {
        ReliableTopicConfig reliableTopicConfig = new ReliableTopicConfig(name)
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_OLDEST)
                .setReadBatchSize(42)
                .setStatisticsEnabled(true)
                .addMessageListenerConfig(getMessageListenerConfig_byImplementation());

        driver.getConfig().addReliableTopicConfig(reliableTopicConfig);

        assertConfigurationsEqualOnAllMembers(reliableTopicConfig);
    }

    @Test
    public void testReliableTopicConfig_whenHasExecutor() {
        ReliableTopicConfig reliableTopicConfig = new ReliableTopicConfig(name)
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_OLDEST)
                .setReadBatchSize(42)
                .setStatisticsEnabled(true)
                .setExecutor(new SampleExecutor());

        driver.getConfig().addReliableTopicConfig(reliableTopicConfig);

        assertConfigurationsEqualOnAllMembers(reliableTopicConfig);
    }

    @Test
    public void testPNCounterConfig() {
        PNCounterConfig pnCounterConfig = new PNCounterConfig(name)
                .setReplicaCount(NON_DEFAULT_BACKUP_COUNT)
                .setStatisticsEnabled(false);

        driver.getConfig().addPNCounterConfig(pnCounterConfig);
        assertConfigurationsEqualOnAllMembers(pnCounterConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(PNCounterConfig config) {
        String name = config.getName();
        for (HazelcastInstance instance : members) {
            PNCounterConfig registeredConfig = instance.getConfig().getPNCounterConfig(name);
            assertEquals(config, registeredConfig);
        }
    }

    @Test
    public void testDataConnectionConfig() {
        Properties properties = new Properties();
        properties.setProperty("prop1", "val1");
        properties.setProperty("prop2", "val2");
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig()
                .setName("some-name")
                .setType(DUMMY_TYPE)
                .setProperties(properties);


        driver.getConfig().addDataConnectionConfig(dataConnectionConfig);
        assertConfigurationsEqualOnAllMembers(dataConnectionConfig);
    }

    /**
     * Reproducer for <a href="https://github.com/hazelcast/hazelcast/issues/24533">GH issue</a>.
     */
    @Test
    public void testDataConnectionConfig_missingType() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig();

        Exception ex = assertThrows(IllegalArgumentException.class,
                () -> driver.getConfig().addDataConnectionConfig(dataConnectionConfig));
        assertThat(ex.getMessage())
                .contains("Data connection type must be non-null and contain text")
                .contains("Data connection name must be non-null and contain text");
    }

    @Test
    public void testDiagnosticsConfig() {
        DiagnosticsConfig config = new DiagnosticsConfig()
                .setEnabled(true)
                .setMaxRolledFileSizeInMB(30)
                .setMaxRolledFileCount(5)
                .setIncludeEpochTime(false)
                .setLogDirectory(Path.of(Diagnostics.DIRECTORY.getDefaultValue(), "logs").toString())
                .setFileNamePrefix("fileNamePrefix")
                .setAutoOffDurationInMinutes(5)
                .setOutputType(DiagnosticsOutputType.STDOUT);

        ((DynamicConfigurationAwareConfig) driver.getConfig()).setDiagnosticsConfig(config);
        assertConfigurationsEqualOnAllMembers(config);
    }

    private void assertConfigurationsEqualOnAllMembers(DataConnectionConfig expectedConfig) {
        assertConfigurationsEqualOnAllMembers(expectedConfig, Config::getDataConnectionConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(CacheSimpleConfig config) {
        assertConfigurationsEqualOnAllMembers(config, Config::getCacheConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(QueueConfig queueConfig) {
        assertConfigurationsEqualOnAllMembers(queueConfig, Config::getQueueConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(CardinalityEstimatorConfig cardinalityEstimatorConfig) {
        assertConfigurationsEqualOnAllMembers(cardinalityEstimatorConfig, Config::getCardinalityEstimatorConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(MultiMapConfig multiMapConfig) {
        assertConfigurationsEqualOnAllMembers(multiMapConfig, Config::getMultiMapConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(ExecutorConfig executorConfig) {
        assertConfigurationsEqualOnAllMembers(executorConfig, Config::getExecutorConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(RingbufferConfig ringbufferConfig) {
        assertConfigurationsEqualOnAllMembers(ringbufferConfig, Config::getRingbufferConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(DurableExecutorConfig durableExecutorConfig) {
        assertConfigurationsEqualOnAllMembers(durableExecutorConfig, Config::getDurableExecutorConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(ScheduledExecutorConfig scheduledExecutorConfig) {
        assertConfigurationsEqualOnAllMembers(scheduledExecutorConfig, Config::getScheduledExecutorConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(SetConfig setConfig) {
        assertConfigurationsEqualOnAllMembers(setConfig, Config::getSetConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(MapConfig mapConfig) {
        assertConfigurationsEqualOnAllMembers(mapConfig, Config::getMapConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(ReplicatedMapConfig replicatedMapConfig) {
        assertConfigurationsEqualOnAllMembers(replicatedMapConfig, Config::getReplicatedMapConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(ListConfig listConfig) {
        assertConfigurationsEqualOnAllMembers(listConfig, Config::getListConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(FlakeIdGeneratorConfig config) {
        assertConfigurationsEqualOnAllMembers(config, Config::getFlakeIdGeneratorConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(TopicConfig topicConfig) {
        assertConfigurationsEqualOnAllMembers(topicConfig, Config::getTopicConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(ReliableTopicConfig reliableTopicConfig) {
        assertConfigurationsEqualOnAllMembers(reliableTopicConfig, Config::getReliableTopicConfig);
    }

    private void assertConfigurationsEqualOnAllMembers(DiagnosticsConfig diagnosticsConfig) {
        for (HazelcastInstance instance : members) {
            DiagnosticsConfig registeredConfig = ((DynamicConfigurationAwareConfig) instance.getConfig()).getDiagnosticsConfig();
            assertThat(registeredConfig).isEqualTo(diagnosticsConfig);
        }
    }

    private <T extends NamedConfig> void assertConfigurationsEqualOnAllMembers(T expectedConfig, BiFunction<Config, String, T> getterByName) {
        for (HazelcastInstance instance : members) {
            T registeredConfig = getterByName.apply(instance.getConfig(), expectedConfig.getName());
            assertThat(registeredConfig).isEqualTo(expectedConfig);
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
                .setSplitBrainProtectionName("split-brain-protection")
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
                .setHotRestartConfig(new HotRestartConfig().setEnabled(true).setFsync(true))
                .setEventJournalConfig(new EventJournalConfig().setEnabled(true)
                        .setCapacity(42)
                        .setTimeToLiveSeconds(52));

        config.setWanReplicationRef(new WanReplicationRef(randomName(), "com.hazelcast.MergePolicy",
                Collections.singletonList("filter"), true));
        config.getMergePolicyConfig().setPolicy("mergePolicy");
        config.setDisablePerEntryInvalidationEvents(true);
        return config;
    }

    private EvictionConfig getEvictionConfigByPolicy() {
        return new EvictionConfig().setSize(39)
                .setMaxSizePolicy(ENTRY_COUNT)
                .setEvictionPolicy(EvictionPolicy.RANDOM);
    }

    private EvictionConfig getEvictionConfigByImplementation() {
        return new EvictionConfig().setSize(39)
                .setMaxSizePolicy(ENTRY_COUNT)
                .setComparator(new SampleEvictionPolicyComparator());
    }

    private SetConfig getSetConfig(String name) {
        return new SetConfig(name)
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setMaxSize(99)
                .setStatisticsEnabled(true)
                .setMergePolicyConfig(new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY,
                        NON_DEFAULT_MERGE_BATCH_SIZE));
    }

    private MapConfig getMapConfig() {
        MapConfig mapConfig = new MapConfig(name);
        mapConfig.setAsyncBackupCount(3)
                .setBackupCount(2)
                .setCacheDeserializedValues(CacheDeserializedValues.ALWAYS)
                .setMerkleTreeConfig(new MerkleTreeConfig().setEnabled(true).setDepth(15))
                .setEventJournalConfig(new EventJournalConfig().setEnabled(true)
                        .setCapacity(42)
                        .setTimeToLiveSeconds(52))
                .setHotRestartConfig(new HotRestartConfig().setEnabled(true).setFsync(true))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setMergePolicyConfig(new MergePolicyConfig(NON_DEFAULT_MERGE_POLICY, NON_DEFAULT_MERGE_BATCH_SIZE))
                .setTimeToLiveSeconds(220)
                .setMaxIdleSeconds(110)
                .setSplitBrainProtectionName(randomString())
                .addAttributeConfig(new AttributeConfig("attributeName", "com.attribute.extractor"))
                .addIndexConfig(new IndexConfig(IndexType.SORTED, "attr"))
                .setMetadataPolicy(MetadataPolicy.OFF)
                .setReadBackupData(true)
                .setStatisticsEnabled(false)
                .setPerEntryStatsEnabled(true);

        mapConfig.getEvictionConfig()
                .setEvictionPolicy(EvictionPolicy.RANDOM)
                .setSize(4096)
                .setMaxSizePolicy(MaxSizePolicy.PER_NODE);
        return mapConfig;
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
                .setSerializeKeys(true)
                .setPredicateConfig(new PredicateConfig("com.hazelcast.Predicate"))
                .setEvictionConfig(new EvictionConfig().setSize(32)
                        .setMaxSizePolicy(ENTRY_COUNT)
                        .setComparatorClassName("com.hazelcast.Comparator"));
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
                .setSplitBrainProtectionName("mySplitBrainProtection")
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
                .setSplitBrainProtectionName("mySplitBrainProtection");
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

    public static class SampleEvictionPolicyComparator
            implements EvictionPolicyComparator<Object, Object, EvictableEntryView<Object, Object>> {

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

    private static class SampleMessageListener implements MessageListener<Object>, Serializable {

        @Override
        public void onMessage(Message<Object> message) {
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleMessageListener);
        }
    }

    private static class SampleExecutor implements Executor, Serializable {

        @Override
        public void execute(Runnable command) {
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleExecutor);
        }
    }
}
