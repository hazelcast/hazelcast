/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.config.mergepolicies.ComplexCustomMergePolicy;
import com.hazelcast.spi.merge.MergingCosts;
import com.hazelcast.spi.merge.MergingExpirationTime;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the integration of the {@link MergePolicyValidator}
 * into the proxy creation of split-brain capable data structures.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MergePolicyValidatorCachingProviderIntegrationTest
        extends AbstractMergePolicyValidatorIntegrationTest {

    @BeforeClass
    public static void jsrSetup() {
        JsrTestUtil.setup();
    }

    @AfterClass
    public static void jsrTeardown() {
        JsrTestUtil.cleanup();
    }

    @Override
    void addConfig(Config config, String name, MergePolicyConfig mergePolicyConfig) {
    }

    private void getCache(String name, MergePolicyConfig mergePolicyConfig) {
        HazelcastInstance hz = getHazelcastInstance(name, mergePolicyConfig);
        HazelcastServerCachingProvider cachingProvider = createServerCachingProvider(hz);
        CacheManager cacheManager = cachingProvider.getCacheManager();

        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setName(name);
        cacheConfig.setStatisticsEnabled(false);
        cacheConfig.getMergePolicyConfig().setPolicy(mergePolicyConfig.getPolicy());

        cacheManager.createCache(name, cacheConfig);
    }

    @Test
    public void testCache_withPutIfAbsentMergePolicy() {
        getCache("putIfAbsent", putIfAbsentMergePolicy);
    }

    @Test
    public void testCache_withHyperLogLogMergePolicy() {
        expectCardinalityEstimatorException(() -> getCache("cardinalityEstimator", hyperLogLogMergePolicy));
    }

    @Test
    public void testCache_withHigherHitsMergePolicy() {
        getCache("higherHits", higherHitsMergePolicy);
    }

    @Test
    public void testCache_withInvalidMergePolicy() {
        expectedInvalidMergePolicyException(() -> getCache("invalid", invalidMergePolicyConfig));
    }

    @Test
    public void testCache_withExpirationTimeMergePolicy() {
        getCache("expirationTime", expirationTimeMergePolicy);
    }

    /**
     * ICache provides only the required {@link MergingExpirationTime},
     * but not the required {@link MergingCosts} from the
     * {@link ComplexCustomMergePolicy}.
     * <p>
     * The thrown exception should contain the merge policy name
     * and the missing merge type.
     */
    @Test
    public void testCache_withComplexCustomMergePolicy() {
        assertThatThrownBy(() -> getCache("complexCustom", complexCustomMergePolicy))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(complexCustomMergePolicy.getPolicy())
                .hasMessageContaining(MergingCosts.class.getName());
    }

    /**
     * ICache provides only some of the required merge types
     * of {@link MapMergeTypes}.
     * <p>
     * The thrown exception should contain the merge policy name
     * and the missing merge type.
     */
    @Test
    public void testCache_withCustomMapMergePolicyNoTypeVariable() {
        assertThatThrownBy(() -> getCache("customMapNoTypeVariable", customMapMergePolicyNoTypeVariable))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(customMapMergePolicyNoTypeVariable.getPolicy())
                .hasMessageContaining(MapMergeTypes.class.getName());
    }

    /**
     * ICache provides only some of the required merge types
     * of {@link MapMergeTypes}.
     * <p>
     * The thrown exception should contain the merge policy name
     * and the missing merge type.
     */
    @Test
    public void testCache_withCustomMapMergePolicy() {
        assertThatThrownBy(() -> getCache("customMap", customMapMergePolicy))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(customMapMergePolicy.getPolicy())
                .hasMessageContaining(MapMergeTypes.class.getName());
    }

    @Test
    public void testCache_withLegacyPutIfAbsentMergePolicy() {
        MergePolicyConfig legacyMergePolicyConfig = new MergePolicyConfig()
                .setPolicy(PutIfAbsentMergePolicy.class.getName());

        getCache("legacyPutIfAbsent", legacyMergePolicyConfig);
    }

    @Override
    void expectCardinalityEstimatorException(ThrowingCallable toRun) {
        assertThatThrownBy(toRun)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CardinalityEstimator");
    }

    @Override
    void expectedInvalidMergePolicyException(ThrowingCallable toRun) {
        assertThatThrownBy(toRun)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(invalidMergePolicyConfig.getPolicy());
    }
}
