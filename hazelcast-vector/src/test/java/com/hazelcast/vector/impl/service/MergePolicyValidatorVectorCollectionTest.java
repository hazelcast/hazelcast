/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.service;


import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.internal.config.AbstractMergePolicyValidatorIntegrationTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MergePolicyValidatorVectorCollectionTest extends AbstractMergePolicyValidatorIntegrationTest {

    @Override
    protected void addConfig(Config config, String name, MergePolicyConfig mergePolicyConfig) {
        var vcConfig = new VectorCollectionConfig(name)
                .addVectorIndexConfig(new VectorIndexConfig("index", Metric.COSINE, 2));
        vcConfig.setMergePolicyConfig(mergePolicyConfig);
        config.addVectorCollectionConfig(vcConfig);
    }

    @Test
    public void testWithPutIfAbsentMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("putIfAbsent", putIfAbsentMergePolicy);
        hz.getVectorCollection("putIfAbsent");
    }

    @Test
    public void testWithPassThroughMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("passThrough", passThroughMergePolicy);
        hz.getVectorCollection("passThrough");
    }

    @Test
    public void testWithDiscardMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("discard", putIfAbsentMergePolicy);
        hz.getVectorCollection("discard");
    }

    @Test
    public void testWithHigherHitsMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("higherHits", higherHitsMergePolicy);
        expectedHigherHitsException(() -> hz.getVectorCollection("higherHits"));
    }

    @Test
    public void testWithExpirationTimeMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("expirationTime", expirationTimeMergePolicy);
        expectInvalidConfigurationException(() -> hz.getVectorCollection("expirationTime"),
                expirationTimeMergePolicy);
    }

    @Test
    public void testWithLastStoredTimeMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("lastStoredTime", lastStoredTimeMergePolicy);
        expectInvalidConfigurationException(() -> hz.getVectorCollection("lastStoredTime"),
                lastStoredTimeMergePolicy);
    }

    @Test
    public void testWithLastStoredTimeNoTypeVarMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("lastStoredTimeNoTypeVar", lastStoredTimeMergePolicyNoTypeVariable);
        expectInvalidConfigurationException(() -> hz.getVectorCollection("lastStoredTimeNoTypeVar"),
                lastStoredTimeMergePolicyNoTypeVariable);
    }

    @Test
    public void testWithInvalidMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("invalid", invalidMergePolicyConfig);
        expectedInvalidMergePolicyException(() -> hz.getVectorCollection("invalid"));
    }

    @Test
    public void testWithComplexCustomMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("custom", complexCustomMergePolicy);
        expectInvalidConfigurationException(() -> hz.getVectorCollection("custom"),
                complexCustomMergePolicy);
    }

    @Test
    public void testWithHyperLogLogMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("hyperLogLog", hyperLogLogMergePolicy);
        expectCardinalityEstimatorException(() -> hz.getVectorCollection("hyperLogLog"));
    }
}
