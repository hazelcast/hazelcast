/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.LockConfig;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.MultiMapConfig.ValueCollectionType.LIST;
import static org.junit.Assert.assertEquals;

// todo tests still missing
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DynamicConfigTest extends HazelcastTestSupport {

    protected static final int INSTANCE_COUNT = 2;

    private String name = randomString();
    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance[] members;
    // add***Config is invoked on driver instance
    private HazelcastInstance driver;

    @Before
    public void setup() {
        members = newInstances();
        driver = getDriver();
    }

    protected HazelcastInstance[] newInstances() {
        factory = createHazelcastInstanceFactory(INSTANCE_COUNT);
        HazelcastInstance[] instances = factory.newInstances();
        return instances;
    }

    protected HazelcastInstance getDriver() {
        return members[members.length - 1];
    }

    @Test
    public void testMultiMapConfig() {
        MultiMapConfig multiMapConfig = new MultiMapConfig(name);
        multiMapConfig.setBackupCount(4)
                      .setAsyncBackupCount(2)
                      .setStatisticsEnabled(true)
                      .setBinary(true)
                      .setValueCollectionType(LIST)
                      .addEntryListenerConfig(
                              new EntryListenerConfig("com.hazelcast.Listener", true, false)
                      );

        driver.getConfig().addMultiMapConfig(multiMapConfig);

        assertConfigurationsEqualsOnAllMembers(multiMapConfig);
    }

    @Test
    public void testMultiMapConfig_whenEntryListenerConfigHasImplementation() {
        MultiMapConfig multiMapConfig = new MultiMapConfig(name);
        multiMapConfig.setBackupCount(4)
                      .setAsyncBackupCount(2)
                      .setStatisticsEnabled(true)
                      .setBinary(true)
                      .setValueCollectionType(LIST)
                      .addEntryListenerConfig(
                              new EntryListenerConfig(new SampleEntryListener(), true, false)
                      );

        driver.getConfig().addMultiMapConfig(multiMapConfig);


        assertConfigurationsEqualsOnAllMembers(multiMapConfig);
    }

    @Test
    public void testCardinalityEstimatorConfig() {
        CardinalityEstimatorConfig config = new CardinalityEstimatorConfig(name, 4 ,2);

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
    public void testListConfig_withItemListenerConfig_byClassName() {
        ListConfig config = getListConfig();
        List<ItemListenerConfig> itemListenerConfigs = new ArrayList<ItemListenerConfig>();
        ItemListenerConfig listenerConfig = new ItemListenerConfig("com.hazelcast.ItemListener", true);
        itemListenerConfigs.add(listenerConfig);
        config.setItemListenerConfigs(itemListenerConfigs);

        driver.getConfig().addListConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testListConfig_withItemListenerConfig_byImplementation() {
        ListConfig config = getListConfig();
        List<ItemListenerConfig> itemListenerConfigs = new ArrayList<ItemListenerConfig>();
        ItemListenerConfig listenerConfig = new ItemListenerConfig(new SampleItemListener(), true);
        itemListenerConfigs.add(listenerConfig);
        config.setItemListenerConfigs(itemListenerConfigs);

        driver.getConfig().addListConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testExecutorConfig() {
        ExecutorConfig config = new ExecutorConfig(name, 7);
        config.setStatisticsEnabled(true);
        config.setQueueCapacity(13);

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
        ScheduledExecutorConfig config = new ScheduledExecutorConfig(name, 2, 3, 10);

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
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byClassName() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig().setEnabled(true).setClassName("com.hazelcast.Foo");

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byFactoryClassName() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig().setEnabled(true).setFactoryClassName("com.hazelcast.FactoryFoo");

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byStoreImplementation() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig().setEnabled(true).setStoreImplementation(new SampleRingbufferStore());

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byFactoryImplementation() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig().setEnabled(true).setFactoryImplementation(new SampleRingbufferStoreFactory());

        driver.getConfig().addRingBufferConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testReplicatedMapConfig_withListenerByClassName() {
        ReplicatedMapConfig config = new ReplicatedMapConfig(name);
        config.setStatisticsEnabled(true);
        config.setMergePolicy("com.hazelcast.SomeMergePolicy");
        config.setInMemoryFormat(InMemoryFormat.NATIVE);
        config.setAsyncFillup(true);
        config.addEntryListenerConfig(new EntryListenerConfig(randomString(), true, false));

        driver.getConfig().addReplicatedMapConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testReplicatedMapConfig_withListenerByImplementation() {
        ReplicatedMapConfig config = new ReplicatedMapConfig(name);
        config.setStatisticsEnabled(true);
        config.setMergePolicy("com.hazelcast.SomeMergePolicy");
        config.setInMemoryFormat(InMemoryFormat.NATIVE);
        config.setAsyncFillup(true);
        config.addEntryListenerConfig(new EntryListenerConfig(new SampleEntryListener(), false, true));

        driver.getConfig().addReplicatedMapConfig(config);

        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testSetConfig() {
        String name = randomName();
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
        MapConfig config = getMapConfig();
        config.addMapPartitionLostListenerConfig(getMapPartitionLostListenerConfig_byImplementation());
        driver.getConfig().addMapConfig(config);
        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testDefaultCacheConfig() {
        CacheSimpleConfig config = new CacheSimpleConfig();
        config.setName(name);
        driver.getConfig().addCacheConfig(config);
        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testCacheConfig() {
        CacheSimpleConfig config = getCacheConfig();
        config.setExpiryPolicyFactoryConfig(new ExpiryPolicyFactoryConfig("expiryPolicyFactory"));
        driver.getConfig().addCacheConfig(config);
        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withEvictionPolicy_cacheLoaderAndWriter() {
        CacheSimpleConfig config = getCacheConfig();
        config.setEvictionConfig(getEvictionConfigByPolicy());
        config.setCacheLoader("com.hazelcast.CacheLoader");
        config.setCacheWriter("com.hazelcast.CacheWriter");
        // also exercise alternative method to set expiry policy factory config
        config.setExpiryPolicyFactory("expiryPolicyFactory");
        driver.getConfig().addCacheConfig(config);
        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withEvictionPolicyImplementation_cacheLoaderAndWriterFactory() {
        CacheSimpleConfig config = getCacheConfig();
        config.setEvictionConfig(getEvictionConfigByImplementation());
        config.setCacheLoaderFactory("com.hazelcast.CacheLoaderFactory");
        config.setCacheWriterFactory("com.hazelcast.CacheWriterFactory");
        driver.getConfig().addCacheConfig(config);
        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withTimedExpiryPolicyFactory() {
        CacheSimpleConfig config = getCacheConfig();
        config.setExpiryPolicyFactoryConfig(new ExpiryPolicyFactoryConfig(
                new TimedExpiryPolicyFactoryConfig(TimedExpiryPolicyFactoryConfig.ExpiryPolicyType.TOUCHED,
                        new ExpiryPolicyFactoryConfig.DurationConfig(130, TimeUnit.SECONDS))
        ));
        driver.getConfig().addCacheConfig(config);
        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withPartitionLostListenerByClassName() {
        CacheSimpleConfig config = getCacheConfig();
        config.addCachePartitionLostListenerConfig(new CachePartitionLostListenerConfig("partitionLostListener"));
        driver.getConfig().addCacheConfig(config);
        assertConfigurationsEqualsOnAllMembers(config);
    }

    @Test
    public void testCacheConfig_withPartitionLostListenerByImplementation() {
        CacheSimpleConfig config = getCacheConfig();
        config.addCachePartitionLostListenerConfig(new CachePartitionLostListenerConfig(new SampleCachePartitionLostListener()));
        driver.getConfig().addCacheConfig(config);
        assertConfigurationsEqualsOnAllMembers(config);
    }

    private CacheSimpleConfig getCacheConfig() {
        CacheSimpleConfig config = new CacheSimpleConfig();
        config.setName(name);
        config.setQuorumName("quorum");
        config.setInMemoryFormat(InMemoryFormat.OBJECT);
        config.setBackupCount(3);
        config.setAsyncBackupCount(2);
        config.setWanReplicationRef(new WanReplicationRef(randomName(), "com.hazelcast.MergePolicy",
                Collections.singletonList("filter"), true));
        CacheSimpleEntryListenerConfig entryListenerConfig = new CacheSimpleEntryListenerConfig();
        entryListenerConfig.setCacheEntryListenerFactory("CacheEntryListenerFactory");
        entryListenerConfig.setSynchronous(true);
        entryListenerConfig.setOldValueRequired(true);
        entryListenerConfig.setCacheEntryEventFilterFactory("CacheEntryEventFilterFactory");
        config.addEntryListenerConfig(entryListenerConfig);
        config.setMergePolicy("mergePolicy");
        config.setStatisticsEnabled(true);
        config.setManagementEnabled(true);
        config.setDisablePerEntryInvalidationEvents(true);
        config.setKeyType("keyType");
        config.setValueType("valueType");
        config.setReadThrough(true);
        config.setWriteThrough(true);
        config.setHotRestartConfig(new HotRestartConfig().setEnabled(true).setFsync(true));
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

    private SetConfig getSetConfig(String name) {
        SetConfig setConfig = new SetConfig(name);
        setConfig.addItemListenerConfig(new ItemListenerConfig("foo.bar.Class", true));
        setConfig.setBackupCount(2);
        return setConfig;
    }

    private MapConfig getMapConfig() {
        MapConfig config = new MapConfig(name);
        config.setAsyncBackupCount(3);
        config.setBackupCount(2);
        config.setCacheDeserializedValues(CacheDeserializedValues.ALWAYS);
        config.setEvictionPolicy(EvictionPolicy.RANDOM);
        config.setHotRestartConfig(new HotRestartConfig().setEnabled(true).setFsync(true));
        config.setInMemoryFormat(InMemoryFormat.OBJECT);
        config.setMergePolicy("com.hazelcast.SomeMergePolicy");
        config.setMaxSizeConfig(new MaxSizeConfig(4096, MaxSizeConfig.MaxSizePolicy.PER_NODE));
        config.setMaxIdleSeconds(110);
        config.setQuorumName(randomString());
        config.addMapAttributeConfig(new MapAttributeConfig("attributeName", "com.attribute.extractor"));
        config.addMapIndexConfig(new MapIndexConfig("attr", true));
        return config;
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
        QueryCacheConfig config = new QueryCacheConfig(randomName())
                .setBatchSize(131)
                .setDelaySeconds(98)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setBufferSize(873)
                .setPopulate(true)
                .setIncludeValue(true)
                .setCoalesce(true)
                .setPredicateConfig(new PredicateConfig("com.hazelcast.Predicate"))
                .setEvictionConfig(
                        new EvictionConfig(32, EvictionConfig.MaxSizePolicy.ENTRY_COUNT, "com.hazelcast.Comparator"));
        return config;
    }

    private MapPartitionLostListenerConfig getMapPartitionLostListenerConfig_byClassName() {
        return new MapPartitionLostListenerConfig("com.hazelcast.PartitionLostListener");
    }

    private MapPartitionLostListenerConfig getMapPartitionLostListenerConfig_byImplementation() {
        return new MapPartitionLostListenerConfig(new SamplePartitionLostListener());
    }

    private ListConfig getListConfig() {
        ListConfig config = new ListConfig(name);
        config.setStatisticsEnabled(true)
              .setMaxSize(99)
              .setBackupCount(4)
              .setAsyncBackupCount(2);
        return config;
    }

    private RingbufferConfig getRingbufferConfig() {
        RingbufferConfig config = new RingbufferConfig(name);
        config.setTimeToLiveSeconds(59);
        config.setInMemoryFormat(InMemoryFormat.OBJECT);
        config.setCapacity(33);
        config.setBackupCount(4);
        config.setAsyncBackupCount(2);
        return config;
    }

    public QueueConfig getQueueConfig() {
        String name = randomName();
        QueueConfig queueConfig = new QueueConfig(name);
        queueConfig.setBackupCount(2);
        queueConfig.setAsyncBackupCount(2);
        // no explicit max size - let's test encoding of the default value
        queueConfig.setEmptyQueueTtl(10);
        queueConfig.setQueueStoreConfig(new QueueStoreConfig().setClassName("foo.bar.ImplName").setEnabled(true));
        queueConfig.setStatisticsEnabled(false);
        queueConfig.setQuorumName("myQuorum");
        return queueConfig;
    }

    public QueueConfig getQueueConfig_withListeners() {
        String name = randomName();
        QueueConfig queueConfig = new QueueConfig(name);
        queueConfig.addItemListenerConfig(new ItemListenerConfig("foo.bar.SampleItemListener", true));
        queueConfig.addItemListenerConfig(new ItemListenerConfig(new SampleItemListener(), false));
        queueConfig.setBackupCount(2);
        queueConfig.setAsyncBackupCount(2);
        queueConfig.setMaxSize(1000);
        queueConfig.setEmptyQueueTtl(10);
        queueConfig.setQueueStoreConfig(new QueueStoreConfig().setClassName("foo.bar.ImplName").setEnabled(true));
        queueConfig.setStatisticsEnabled(false);
        queueConfig.setQuorumName("myQuorum");
        return queueConfig;
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
}
