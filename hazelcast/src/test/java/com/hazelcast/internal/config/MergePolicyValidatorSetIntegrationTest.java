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

import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests the integration of the {@link MergePolicyValidator}
 * into the proxy creation of split-brain capable data structures.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MergePolicyValidatorSetIntegrationTest extends AbstractMergePolicyValidatorIntegrationTest {

    @Override
    void addConfig(Config config, String name, MergePolicyConfig mergePolicyConfig) {
        SetConfig setConfig = new SetConfig(name)
                .setMergePolicyConfig(mergePolicyConfig);

        config.addSetConfig(setConfig);
    }

    @Test
    public void testSet_withPutIfAbsentMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("putIfAbsent", putIfAbsentMergePolicy);

        hz.getSet("putIfAbsent");
    }

    @Test
    public void testSet_withHyperLogLogMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("cardinalityEstimator", hyperLogLogMergePolicy);

        expectCardinalityEstimatorException();
        hz.getSet("cardinalityEstimator");
    }

    @Test
    public void testSet_withHigherHitsMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("higherHits", higherHitsMergePolicy);

        expectedHigherHitsException();
        hz.getSet("higherHits");
    }

    @Test
    public void testSet_withInvalidMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("invalid", invalidMergePolicyConfig);

        expectedInvalidMergePolicyException();
        hz.getSet("invalid");
    }
}
