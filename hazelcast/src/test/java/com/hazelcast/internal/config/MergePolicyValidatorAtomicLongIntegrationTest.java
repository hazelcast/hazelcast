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
import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
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
public class MergePolicyValidatorAtomicLongIntegrationTest extends AbstractMergePolicyValidatorIntegrationTest {

    @Override
    void addConfig(Config config, String name, MergePolicyConfig mergePolicyConfig) {
        AtomicLongConfig atomicLongConfig = new AtomicLongConfig(name)
                .setMergePolicyConfig(mergePolicyConfig);

        config.addAtomicLongConfig(atomicLongConfig);
    }

    @Test
    public void testAtomicLong_withPutIfAbsentMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("putIfAbsent", putIfAbsentMergePolicy);

        hz.getAtomicLong("cardinalityEstimator");
    }

    @Test
    public void testAtomicLong_withHyperLogLogMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("cardinalityEstimator", hyperLogLogMergePolicy);

        expectCardinalityEstimatorException();
        hz.getAtomicLong("cardinalityEstimator");
    }

    @Test
    public void testAtomicLong_withHigherHitsMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("higherHits", higherHitsMergePolicy);

        expectedHigherHitsException();
        hz.getAtomicLong("higherHits");
    }

    @Test
    public void testAtomicLong_withInvalidMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("invalid", invalidMergePolicyConfig);

        expectedInvalidMergePolicyException();
        hz.getAtomicLong("invalid");
    }
}
