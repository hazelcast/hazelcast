/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.config.mergepolicies.ComplexCustomMergePolicy;
import com.hazelcast.internal.config.mergepolicies.CustomMapMergePolicy;
import com.hazelcast.internal.config.mergepolicies.CustomMapMergePolicyNoTypeVariable;
import com.hazelcast.internal.config.mergepolicies.LastStoredTimeMergePolicy;
import com.hazelcast.internal.config.mergepolicies.LastStoredTimeMergePolicyNoTypeVariable;
import com.hazelcast.spi.merge.ExpirationTimeMergePolicy;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.HyperLogLogMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.Before;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the integration of the {@link MergePolicyValidator}
 * into the proxy creation of split-brain capable data structures.
 */
public abstract class AbstractMergePolicyValidatorIntegrationTest extends HazelcastTestSupport {

    MergePolicyConfig putIfAbsentMergePolicy;
    MergePolicyConfig hyperLogLogMergePolicy;
    MergePolicyConfig higherHitsMergePolicy;
    MergePolicyConfig invalidMergePolicyConfig;
    MergePolicyConfig expirationTimeMergePolicy;
    MergePolicyConfig lastStoredTimeMergePolicy;
    MergePolicyConfig lastStoredTimeMergePolicyNoTypeVariable;
    MergePolicyConfig complexCustomMergePolicy;
    MergePolicyConfig customMapMergePolicy;
    MergePolicyConfig customMapMergePolicyNoTypeVariable;

    private TestHazelcastInstanceFactory factory;

    @Before
    public final void setUp() {
        putIfAbsentMergePolicy = new MergePolicyConfig()
                .setPolicy(PutIfAbsentMergePolicy.class.getSimpleName());
        hyperLogLogMergePolicy = new MergePolicyConfig()
                .setPolicy(HyperLogLogMergePolicy.class.getName());
        higherHitsMergePolicy = new MergePolicyConfig()
                .setPolicy(HigherHitsMergePolicy.class.getSimpleName());
        invalidMergePolicyConfig = new MergePolicyConfig()
                .setPolicy("InvalidMergePolicyClass");
        expirationTimeMergePolicy = new MergePolicyConfig()
                .setPolicy(ExpirationTimeMergePolicy.class.getName());
        lastStoredTimeMergePolicy = new MergePolicyConfig()
                .setPolicy(LastStoredTimeMergePolicy.class.getName());
        lastStoredTimeMergePolicyNoTypeVariable = new MergePolicyConfig()
                .setPolicy(LastStoredTimeMergePolicyNoTypeVariable.class.getName());
        complexCustomMergePolicy = new MergePolicyConfig()
                .setPolicy(ComplexCustomMergePolicy.class.getName());
        customMapMergePolicy = new MergePolicyConfig()
                .setPolicy(CustomMapMergePolicy.class.getName());
        customMapMergePolicyNoTypeVariable = new MergePolicyConfig()
                .setPolicy(CustomMapMergePolicyNoTypeVariable.class.getName());

        factory = createHazelcastInstanceFactory();
    }

    abstract void addConfig(Config config, String name, MergePolicyConfig mergePolicyConfig);

    HazelcastInstance getHazelcastInstance(String name, MergePolicyConfig mergePolicyConfig) {
        Config config = smallInstanceConfig();
        addConfig(config, name, mergePolicyConfig);

        return factory.newHazelcastInstance(config);
    }

    void expectCardinalityEstimatorException(ThrowingCallable toRun) {
        assertThatThrownBy(toRun)
                .isInstanceOf(InvalidConfigurationException.class)
                        .hasMessageContaining("CardinalityEstimator");
    }

    void expectedHigherHitsException(ThrowingCallable toRun) {
        assertThatThrownBy(toRun)
                .isInstanceOf(InvalidConfigurationException.class)
                        .hasMessageContaining(higherHitsMergePolicy.getPolicy());
    }

    void expectedInvalidMergePolicyException(ThrowingCallable toRun) {
        assertThatThrownBy(toRun)
                .isInstanceOf(InvalidConfigurationException.class)
                        .hasMessageContaining(invalidMergePolicyConfig.getPolicy());
    }

    void expectedMapStatisticsDisabledException(MergePolicyConfig mergePolicyConfig, ThrowingCallable toRun) {
        assertThatThrownBy(toRun)
                .isInstanceOf(InvalidConfigurationException.class)
                        .hasMessageContaining(mergePolicyConfig.getPolicy())
                                .hasMessageContaining("perEntryStatsEnabled field of map-config");
    }
}
