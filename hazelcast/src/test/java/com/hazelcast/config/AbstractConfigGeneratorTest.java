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
