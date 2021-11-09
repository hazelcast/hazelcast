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
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.topic.TopicOverloadPolicy;
import org.junit.Test;

import java.util.Properties;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public abstract class AbstractConfigGeneratorTest extends HazelcastTestSupport {

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

    private static class TestRingbufferStore implements RingbufferStore {
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

    private static class TestRingbufferStoreFactory implements RingbufferStoreFactory {
        @Override
        public RingbufferStore newRingbufferStore(String name, Properties properties) {
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

        ScheduledExecutorConfig defaultSchedExecConfig = new ScheduledExecutorConfig();
        cfg.addScheduledExecutorConfig(defaultSchedExecConfig);

        ScheduledExecutorConfig existing = getNewConfigViaGenerator(cfg).getScheduledExecutorConfig("Existing");
        assertEquals(scheduledExecutorConfig, existing);

        ScheduledExecutorConfig fallsbackToDefault = getNewConfigViaGenerator(cfg)
                .getScheduledExecutorConfig("NotExisting/Default");
        assertEquals(defaultSchedExecConfig.getMergePolicyConfig(), fallsbackToDefault.getMergePolicyConfig());
        assertEquals(defaultSchedExecConfig.getCapacity(), fallsbackToDefault.getCapacity());
        assertEquals(defaultSchedExecConfig.getCapacityPolicy(), fallsbackToDefault.getCapacityPolicy());
        assertEquals(defaultSchedExecConfig.getPoolSize(), fallsbackToDefault.getPoolSize());
        assertEquals(defaultSchedExecConfig.getDurability(), fallsbackToDefault.getDurability());
        assertEquals(defaultSchedExecConfig.isStatisticsEnabled(), fallsbackToDefault.isStatisticsEnabled());
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

        CardinalityEstimatorConfig fallsbackToDefault = getNewConfigViaGenerator(cfg)
                .getCardinalityEstimatorConfig("NotExisting/Default");
        assertEquals(defaultCardinalityEstConfig.getMergePolicyConfig(), fallsbackToDefault.getMergePolicyConfig());
        assertEquals(defaultCardinalityEstConfig.getBackupCount(), fallsbackToDefault.getBackupCount());
        assertEquals(defaultCardinalityEstConfig.getAsyncBackupCount(), fallsbackToDefault.getAsyncBackupCount());
        assertEquals(defaultCardinalityEstConfig.getSplitBrainProtectionName(), fallsbackToDefault.getSplitBrainProtectionName());
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
