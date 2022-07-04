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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.config.mergepolicies.ComplexCustomMergePolicy;
import com.hazelcast.internal.config.mergepolicies.CustomMapMergePolicy;
import com.hazelcast.spi.merge.MergingCosts;
import com.hazelcast.spi.merge.MergingExpirationTime;
import com.hazelcast.spi.merge.MergingLastStoredTime;
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
public class MergePolicyValidatorMapIntegrationTest extends AbstractMergePolicyValidatorIntegrationTest {

    private boolean perEntryStatsEnabled = true;

    @Override
    void addConfig(Config config, String name, MergePolicyConfig mergePolicyConfig) {
        MapConfig mapConfig = new MapConfig(name)
                .setPerEntryStatsEnabled(perEntryStatsEnabled)
                .setMergePolicyConfig(mergePolicyConfig);

        config.addMapConfig(mapConfig);
    }

    @Test
    public void testMap_withPutIfAbsentMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("putIfAbsent", putIfAbsentMergePolicy);

        hz.getMap("putIfAbsent");
    }

    @Test
    public void testMap_withHyperLogLogMergePolicy() {
        expectCardinalityEstimatorException();
        HazelcastInstance hz = getHazelcastInstance("cardinalityEstimator", hyperLogLogMergePolicy);

        hz.getMap("cardinalityEstimator");
    }

    @Test
    public void testMap_withHigherHitsMergePolicy() {
        perEntryStatsEnabled = true;
        HazelcastInstance hz = getHazelcastInstance("higherHits", higherHitsMergePolicy);

        hz.getMap("higherHits");
    }

    @Test
    public void testMap_withInvalidMergePolicy() {
        expectedInvalidMergePolicyException();
        HazelcastInstance hz = getHazelcastInstance("invalid", invalidMergePolicyConfig);

        hz.getMap("invalid");
    }

    /**
     * The {@link MergingExpirationTime} is just provided if map statistics are enabled.
     */
    @Test
    public void testMap_withExpirationTimeMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("expirationTime", expirationTimeMergePolicy);

        hz.getMap("expirationTime");
    }

    @Test
    public void testMap_withExpirationTimeMergePolicy_withStatsEnabled() {
        perEntryStatsEnabled = true;
        HazelcastInstance hz = getHazelcastInstance("expirationTime", expirationTimeMergePolicy);

        hz.getMap("expirationTime");
    }

    /**
     * The {@link MergingLastStoredTime} is just provided if map statistics are enabled.
     */
    @Test
    public void testMap_withLastStoredTimeMergePolicy() {
        perEntryStatsEnabled = false;
        expectedMapStatisticsDisabledException(lastStoredTimeMergePolicy);

        HazelcastInstance hz = getHazelcastInstance("lastStoredTime", lastStoredTimeMergePolicy);

        hz.getMap("lastStoredTime");
    }

    @Test
    public void testMap_withLastStoredMergePolicy_withStatsEnabled() {
        perEntryStatsEnabled = true;
        HazelcastInstance hz = getHazelcastInstance("lastStoredTime", lastStoredTimeMergePolicy);

        hz.getMap("lastStoredTime");
    }

    /**
     * The {@link MergingLastStoredTime} is just provided if map statistics are enabled.
     */
    @Test
    public void testMap_withLastStoredTimeMergePolicyNoTypeVariable() {
        perEntryStatsEnabled = false;
        expectedMapStatisticsDisabledException(lastStoredTimeMergePolicyNoTypeVariable);

        HazelcastInstance hz = getHazelcastInstance("lastStoredTimeNoTypeVariable", lastStoredTimeMergePolicyNoTypeVariable);

        hz.getMap("lastStoredTimeNoTypeVariable");
    }

    @Test
    public void testMap_withLastStoredMergePolicyNoTypeVariable_withStatsEnabled() {
        perEntryStatsEnabled = true;
        HazelcastInstance hz = getHazelcastInstance("lastStoredTimeNoTypeVariable", lastStoredTimeMergePolicyNoTypeVariable);

        hz.getMap("lastStoredTimeNoTypeVariable");
    }

    /**
     * IMap always provides the required {@link MergingCosts} from the {@link ComplexCustomMergePolicy},
     * but the required {@link MergingExpirationTime} are just provided if map statistics are enabled.
     */
    @Test
    public void testMap_withComplexCustomMergePolicy() {
        HazelcastInstance hz = getHazelcastInstance("complexCustom", complexCustomMergePolicy);

        hz.getMap("complexCustom");
    }

    @Test
    public void testMap_withComplexCustomMergePolicy_withStatsEnabled() {
        perEntryStatsEnabled = true;
        HazelcastInstance hz = getHazelcastInstance("complexCustom", complexCustomMergePolicy);

        hz.getMap("complexCustom");
    }

    /**
     * The required {@link MergingExpirationTime} and {@link MergingLastStoredTime} from the {@link CustomMapMergePolicy}
     * are just provided if map statistics are enabled.
     */
    @Test
    public void testMap_withCustomMapMergePolicy() {
        perEntryStatsEnabled = false;
        expectedMapStatisticsDisabledException(customMapMergePolicy);
        HazelcastInstance hz = getHazelcastInstance("customMap", customMapMergePolicy);

        hz.getMap("customMap");
    }

    @Test
    public void testMap_withCustomMapMergePolicy_withStatsEnabled() {
        perEntryStatsEnabled = true;
        HazelcastInstance hz = getHazelcastInstance("customMap", customMapMergePolicy);

        hz.getMap("customMap");
    }

    /**
     * The required {@link MergingExpirationTime} and {@link MergingLastStoredTime} from the {@link CustomMapMergePolicy}
     * are just provided if map statistics are enabled.
     */
    @Test
    public void testMap_withCustomMapMergePolicyNoTypeVariable() {
        perEntryStatsEnabled = false;
        expectedMapStatisticsDisabledException(customMapMergePolicyNoTypeVariable);

        HazelcastInstance hz = getHazelcastInstance("customMapNoTypeVariable", customMapMergePolicyNoTypeVariable);

        hz.getMap("customMapNoTypeVariable");
    }

    @Test
    public void testMap_withCustomMapMergePolicyNoTypeVariable_withStatsEnabled() {
        perEntryStatsEnabled = true;
        HazelcastInstance hz = getHazelcastInstance("customMapNoTypeVariable", customMapMergePolicyNoTypeVariable);

        hz.getMap("customMapNoTypeVariable");
    }
}
