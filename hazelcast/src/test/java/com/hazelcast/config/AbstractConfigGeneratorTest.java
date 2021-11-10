/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.ringbuffer.RingbufferStore;
import com.hazelcast.ringbuffer.RingbufferStoreFactory;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.topic.TopicOverloadPolicy;
import org.junit.Test;

import java.util.Properties;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class AbstractConfigGeneratorTest extends HazelcastTestSupport {

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

        Config xmlConfig = getNewConfigViaGenerator(config);

        ListConfig actualConfig = xmlConfig.getListConfig("testList");
        assertEquals(expectedConfig, actualConfig);
    }

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

        Config xmlConfig = getNewConfigViaGenerator(config);

        SetConfig actualConfig = xmlConfig.getSetConfig("testSet");
        assertEquals(expectedConfig, actualConfig);
    }

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

        Config xmlConfig = getNewConfigViaGenerator(config);

        MultiMapConfig actualConfig = xmlConfig.getMultiMapConfig(expectedConfig.getName());
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
        Config xmlConfig = getNewConfigViaGenerator(config);

        assertEquals(multiMapConfig, xmlConfig.getMultiMapConfig("myMultiMap"));
    }

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

        Config xmlConfig = getNewConfigViaGenerator(config);

        ReplicatedMapConfig xmlReplicatedMapConfig = xmlConfig.getReplicatedMapConfig("replicated-map-name");
        MergePolicyConfig actualMergePolicyConfig = xmlReplicatedMapConfig.getMergePolicyConfig();
        assertEquals("replicated-map-name", xmlReplicatedMapConfig.getName());
        assertFalse(xmlReplicatedMapConfig.isStatisticsEnabled());
        assertEquals("com.hazelcast.entrylistener", xmlReplicatedMapConfig.getListenerConfigs().get(0).getClassName());
        assertEquals("splitBrainProtection", xmlReplicatedMapConfig.getSplitBrainProtectionName());
        assertEquals(InMemoryFormat.NATIVE, xmlReplicatedMapConfig.getInMemoryFormat());
        assertTrue(xmlReplicatedMapConfig.isAsyncFillup());
        assertEquals("PassThroughMergePolicy", actualMergePolicyConfig.getPolicy());
        assertEquals(1234, actualMergePolicyConfig.getBatchSize());
        assertEquals(replicatedMapConfig, xmlReplicatedMapConfig);
    }

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

    private static class TestRingbufferStoreFactory implements RingbufferStoreFactory<Object> {
        @Override
        public RingbufferStore<Object> newRingbufferStore(String name, Properties properties) {
            return null;
        }
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

        Config xmlConfig = getNewConfigViaGenerator(config);

        RingbufferConfig actualConfig = xmlConfig.getRingbufferConfig(expectedConfig.getName());
        ConfigCompatibilityChecker.checkRingbufferConfig(expectedConfig, actualConfig);
    }

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

        Config xmlConfig = getNewConfigViaGenerator(config);

        ExecutorConfig actualConfig = xmlConfig.getExecutorConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

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

        Config xmlConfig = getNewConfigViaGenerator(config);

        DurableExecutorConfig actualConfig = xmlConfig.getDurableExecutorConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

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

    @Test
    public void testPNCounter() {
        PNCounterConfig expectedConfig = new PNCounterConfig()
                .setName("testPNCounter")
                .setReplicaCount(100)
                .setSplitBrainProtectionName("splitBrainProtection");

        Config config = new Config().addPNCounterConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaGenerator(config);

        PNCounterConfig actualConfig = xmlConfig.getPNCounterConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

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

        Config xmlConfig = getNewConfigViaGenerator(config);

        FlakeIdGeneratorConfig xmlReplicatedConfig = xmlConfig.getFlakeIdGeneratorConfig("flake-id-gen1");
        assertEquals(figConfig, xmlReplicatedConfig);
    }

    abstract Config getNewConfigViaGenerator(Config config);
}
