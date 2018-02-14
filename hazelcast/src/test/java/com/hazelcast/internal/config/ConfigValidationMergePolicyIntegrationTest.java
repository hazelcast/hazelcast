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

package com.hazelcast.internal.config;

import com.hazelcast.config.AtomicLongConfig;
import com.hazelcast.config.AtomicReferenceConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.HyperLogLogMergePolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.hamcrest.CoreMatchers.containsString;

/**
 * Tests the integration of the {@link ConfigValidator} into the proxy creation of split-brain capable data structures.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConfigValidationMergePolicyIntegrationTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private MergePolicyConfig hyperLogLogMergePolicy;
    private MergePolicyConfig statisticsMergePolicy;

    private TestHazelcastInstanceFactory factory;

    @Before
    public void setUp() {
        hyperLogLogMergePolicy = new MergePolicyConfig()
                .setPolicy(HyperLogLogMergePolicy.class.getSimpleName());
        statisticsMergePolicy = new MergePolicyConfig()
                .setPolicy(HigherHitsMergePolicy.class.getName());

        factory = createHazelcastInstanceFactory();
    }

    @Test
    public void testCache_withHyperLogLogMergePolicy() {
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig();
        cacheSimpleConfig.setName("cardinalityEstimator");
        cacheSimpleConfig.setMergePolicy(hyperLogLogMergePolicy.getPolicy());

        Config config = new Config()
                .addCacheConfig(cacheSimpleConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectCardinalityEstimatorException();
        hz.getCacheManager().getCache("cardinalityEstimator");
    }

    @Test
    public void testCache_withStatisticsMergePolicy() {
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig();
        cacheSimpleConfig.setName("statistics");
        cacheSimpleConfig.setStatisticsEnabled(false);
        cacheSimpleConfig.setMergePolicy(statisticsMergePolicy.getPolicy());

        Config config = new Config()
                .addCacheConfig(cacheSimpleConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectStatisticsException();
        hz.getCacheManager().getCache("statistics");
    }

    @Test
    public void testMap_withHyperLogLogMergePolicy() {
        MapConfig mapConfig = new MapConfig("cardinalityEstimator")
                .setMergePolicyConfig(hyperLogLogMergePolicy);

        Config config = new Config()
                .addMapConfig(mapConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectCardinalityEstimatorException();
        hz.getMap("cardinalityEstimator");
    }

    @Test
    public void testMap_withStatisticsMergePolicy() {
        MapConfig mapConfig = new MapConfig("statistics")
                .setStatisticsEnabled(false)
                .setMergePolicyConfig(statisticsMergePolicy);

        Config config = new Config()
                .addMapConfig(mapConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectStatisticsException();
        hz.getMap("statistics");
    }

    @Test
    public void testReplicatedMap_withHyperLogLogMergePolicy() {
        ReplicatedMapConfig replicatedMapConfig = new ReplicatedMapConfig("cardinalityEstimator")
                .setMergePolicyConfig(hyperLogLogMergePolicy);

        Config config = new Config()
                .addReplicatedMapConfig(replicatedMapConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectCardinalityEstimatorException();
        hz.getReplicatedMap("cardinalityEstimator");
    }

    @Test
    public void testReplicatedMap_withStatisticsMergePolicy() {
        ReplicatedMapConfig replicatedMapConfig = new ReplicatedMapConfig("statistics")
                .setStatisticsEnabled(false)
                .setMergePolicyConfig(statisticsMergePolicy);

        Config config = new Config()
                .addReplicatedMapConfig(replicatedMapConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectStatisticsException();
        hz.getReplicatedMap("statistics");
    }

    @Test
    public void testMultiMap_withHyperLogLogMergePolicy() {
        MultiMapConfig multiMapConfig = new MultiMapConfig("cardinalityEstimator")
                .setStatisticsEnabled(true)
                .setMergePolicyConfig(hyperLogLogMergePolicy);

        Config config = new Config()
                .addMultiMapConfig(multiMapConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectCardinalityEstimatorException();
        hz.getMultiMap("cardinalityEstimator");
    }

    @Test
    public void testMultiMap_withStatisticsMergePolicy() {
        MultiMapConfig multiMapConfig = new MultiMapConfig("statistics")
                .setStatisticsEnabled(false)
                .setMergePolicyConfig(statisticsMergePolicy);

        Config config = new Config()
                .addMultiMapConfig(multiMapConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectStatisticsException();
        hz.getMultiMap("statistics");
    }

    @Test
    public void testQueue_withHyperLogLogMergePolicy() {
        QueueConfig queueConfig = new QueueConfig("cardinalityEstimator")
                .setMergePolicyConfig(hyperLogLogMergePolicy);

        Config config = new Config()
                .addQueueConfig(queueConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectCardinalityEstimatorException();
        hz.getQueue("cardinalityEstimator");
    }

    @Test
    public void testQueue_withStatisticsMergePolicy() {
        // queue statistics are not suitable for merge policies
        QueueConfig queueConfig = new QueueConfig("statistics")
                .setStatisticsEnabled(true)
                .setMergePolicyConfig(statisticsMergePolicy);

        Config config = new Config()
                .addQueueConfig(queueConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectStatisticsException();
        hz.getQueue("statistics");
    }

    @Test
    public void testList_withHyperLogLogMergePolicy() {
        ListConfig listConfig = new ListConfig("cardinalityEstimator")
                .setMergePolicyConfig(hyperLogLogMergePolicy);

        Config config = new Config()
                .addListConfig(listConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectCardinalityEstimatorException();
        hz.getList("cardinalityEstimator");
    }

    @Test
    public void testList_withStatisticsMergePolicy() {
        // list statistics are not suitable for merge policies
        ListConfig listConfig = new ListConfig("statistics")
                .setStatisticsEnabled(true)
                .setMergePolicyConfig(statisticsMergePolicy);

        Config config = new Config()
                .addListConfig(listConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectStatisticsException();
        hz.getList("statistics");
    }

    @Test
    public void testSet_withHyperLogLogMergePolicy() {
        SetConfig setConfig = new SetConfig("cardinalityEstimator")
                .setMergePolicyConfig(hyperLogLogMergePolicy);

        Config config = new Config()
                .addSetConfig(setConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectCardinalityEstimatorException();
        hz.getSet("cardinalityEstimator");
    }

    @Test
    public void testSet_withStatisticsMergePolicy() {
        // set statistics are not suitable for merge policies
        SetConfig setConfig = new SetConfig("statistics")
                .setStatisticsEnabled(true)
                .setMergePolicyConfig(statisticsMergePolicy);

        Config config = new Config()
                .addSetConfig(setConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectStatisticsException();
        hz.getSet("statistics");
    }

    @Test
    public void testRingbuffer_withHyperLogLogMergePolicy() {
        RingbufferConfig ringbufferConfig = new RingbufferConfig("cardinalityEstimator")
                .setMergePolicyConfig(hyperLogLogMergePolicy);

        Config config = new Config()
                .addRingBufferConfig(ringbufferConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectCardinalityEstimatorException();
        hz.getRingbuffer("cardinalityEstimator");
    }

    @Test
    public void testRingbuffer_withStatisticsMergePolicy() {
        RingbufferConfig ringbufferConfig = new RingbufferConfig("statistics")
                .setMergePolicyConfig(statisticsMergePolicy);

        Config config = new Config()
                .addRingBufferConfig(ringbufferConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectStatisticsException();
        hz.getRingbuffer("statistics");
    }

    @Test
    public void testAtomicLong_withHyperLogLogMergePolicy() {
        AtomicLongConfig atomicLongConfig = new AtomicLongConfig("cardinalityEstimator")
                .setMergePolicyConfig(hyperLogLogMergePolicy);

        Config config = new Config()
                .addAtomicLongConfig(atomicLongConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectCardinalityEstimatorException();
        hz.getAtomicLong("cardinalityEstimator");
    }

    @Test
    public void testAtomicLong_withStatisticsMergePolicy() {
        AtomicLongConfig atomicLongConfig = new AtomicLongConfig("statistics")
                .setMergePolicyConfig(statisticsMergePolicy);

        Config config = new Config()
                .addAtomicLongConfig(atomicLongConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectStatisticsException();
        hz.getAtomicLong("statistics");
    }

    @Test
    public void testAtomicReference_withHyperLogLogMergePolicy() {
        AtomicReferenceConfig atomicReferenceConfig = new AtomicReferenceConfig("cardinalityEstimator")
                .setMergePolicyConfig(hyperLogLogMergePolicy);

        Config config = new Config()
                .addAtomicReferenceConfig(atomicReferenceConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectCardinalityEstimatorException();
        hz.getAtomicReference("cardinalityEstimator");
    }

    @Test
    public void testAtomicReference_withStatisticsMergePolicy() {
        AtomicReferenceConfig atomicReferenceConfig = new AtomicReferenceConfig("statistics")
                .setMergePolicyConfig(statisticsMergePolicy);

        Config config = new Config()
                .addAtomicReferenceConfig(atomicReferenceConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectStatisticsException();
        hz.getAtomicReference("statistics");
    }

    @Test
    public void testScheduledExecutor_withHyperLogLogMergePolicy() {
        ScheduledExecutorConfig scheduledExecutorConfig = new ScheduledExecutorConfig("cardinalityEstimator")
                .setMergePolicyConfig(hyperLogLogMergePolicy);

        Config config = new Config()
                .addScheduledExecutorConfig(scheduledExecutorConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectCardinalityEstimatorException();
        hz.getScheduledExecutorService("cardinalityEstimator");
    }

    @Test
    public void testScheduledExecutor_withStatisticsMergePolicy() {
        ScheduledExecutorConfig scheduledExecutorConfig = new ScheduledExecutorConfig("statistics")
                .setMergePolicyConfig(statisticsMergePolicy);

        Config config = new Config()
                .addScheduledExecutorConfig(scheduledExecutorConfig);

        HazelcastInstance hz = factory.newHazelcastInstance(config);

        expectStatisticsException();
        hz.getScheduledExecutorService("statistics");
    }

    private void expectCardinalityEstimatorException() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(containsString("CardinalityEstimator"));
    }

    private void expectStatisticsException() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(containsString(statisticsMergePolicy.getPolicy()));
    }
}
