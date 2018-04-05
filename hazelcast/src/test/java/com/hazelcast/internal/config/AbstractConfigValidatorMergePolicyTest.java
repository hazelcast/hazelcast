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

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.HyperLogLogMergePolicy;
import com.hazelcast.spi.merge.LatestAccessMergePolicy;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.containsString;

/**
 * Provides a test setup with merge policies, which need statistics implemented and enabled as well as specialized merge policies.
 */
public abstract class AbstractConfigValidatorMergePolicyTest extends HazelcastTestSupport {

    @Parameters(name = "mergePolicy:{2} (stats:{0} (HLL:{1}) (simpleName:{3})")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {false, false, DiscardMergePolicy.class, true},
                {false, false, DiscardMergePolicy.class, false},
                {false, false, PassThroughMergePolicy.class, true},
                {false, false, PassThroughMergePolicy.class, false},
                {false, false, PutIfAbsentMergePolicy.class, true},
                {false, false, PutIfAbsentMergePolicy.class, false},

                {false, true, HyperLogLogMergePolicy.class, true},
                {false, true, HyperLogLogMergePolicy.class, false},

                {true, false, HigherHitsMergePolicy.class, true},
                {true, false, HigherHitsMergePolicy.class, false},
                {true, false, LatestAccessMergePolicy.class, true},
                {true, false, LatestAccessMergePolicy.class, false},
                {true, false, LatestUpdateMergePolicy.class, true},
                {true, false, LatestUpdateMergePolicy.class, false},
        });
    }

    @Parameter
    public boolean requiresStatistics;

    @Parameter(1)
    public boolean requiresCardinalityEstimator;

    @Parameter(2)
    public Class<? extends SplitBrainMergePolicy> mergePolicyClass;

    @Parameter(3)
    public boolean useSimpleName;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    MergePolicyConfig mergePolicyConfig;

    @Before
    public final void prepareConfig() {
        mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(useSimpleName ? mergePolicyClass.getSimpleName() : mergePolicyClass.getName());
    }

    void expectedExceptions() {
        if (requiresCardinalityEstimator) {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage(containsString("CardinalityEstimator"));
        }
    }

    void expectExceptionsWithoutStatistics() {
        if (requiresStatistics) {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage(containsString(mergePolicyConfig.getPolicy()));
        } else if (requiresCardinalityEstimator) {
            expectedException.expect(IllegalArgumentException.class);
            expectedException.expectMessage(containsString("CardinalityEstimator"));
        }
    }
}
