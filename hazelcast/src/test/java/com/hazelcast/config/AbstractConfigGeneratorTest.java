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

import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class AbstractConfigGeneratorTest extends HazelcastTestSupport {

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
