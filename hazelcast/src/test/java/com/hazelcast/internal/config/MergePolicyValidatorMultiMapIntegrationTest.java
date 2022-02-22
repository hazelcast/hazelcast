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

package com.hazelcast.internal.config;

import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests the integration of the {@link MergePolicyValidator}
 * into the proxy creation of split-brain capable data structures.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MergePolicyValidatorMultiMapIntegrationTest extends AbstractMergePolicyValidatorIntegrationTest {

    @Override
    void addConfig(Config config, String name, MergePolicyConfig mergePolicyConfig) {
        MultiMapConfig multiMapConfig = new MultiMapConfig(name)
                .setMergePolicyConfig(mergePolicyConfig);

        config.addMultiMapConfig(multiMapConfig);
    }

    @Test
    public void testMultiMap_withPutIfAbsentMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("putIfAbsent", putIfAbsentMergePolicy);

        hz.getMultiMap("putIfAbsent");
    }

    @Test
    public void testMultiMap_withHyperLogLogMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("cardinalityEstimator", hyperLogLogMergePolicy);

        expectCardinalityEstimatorException();
        hz.getMultiMap("cardinalityEstimator");
    }

    @Test
    public void testMultiMap_withHigherHitsMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("higherHits", higherHitsMergePolicy);

        hz.getMultiMap("higherHits");
    }

    @Test
    public void testMultiMap_withInvalidMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("invalid", invalidMergePolicyConfig);

        expectedInvalidMergePolicyException();
        hz.getMultiMap("invalid");
    }
}
