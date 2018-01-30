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

package com.hazelcast.cardinality;

import com.hazelcast.spi.merge.HyperLogLogMergePolicy;
import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class CardinalityEstimatorSplitBrainTest extends SplitBrainTestSupport {

    private final String name = randomName();

    private final int INITIAL_COUNT = 100000;
    private final int EXTRA_COUNT = 10000;
    private final int TOTAL_COUNT = INITIAL_COUNT + (brains().length * EXTRA_COUNT);

    private MergeLifecycleListener mergeLifecycleListener;
    private long estimate;

    @Parameterized.Parameters(name = "mergePolicy:{0}")
    public static Collection<Object> parameters() {
        return asList(new Object[]{
                DiscardMergePolicy.class,
                PutIfAbsentMergePolicy.class,
                HyperLogLogMergePolicy.class,

        });
    }

    @Parameterized.Parameter
    public Class<? extends SplitBrainMergePolicy> mergePolicyClass;

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mergePolicyClass.getName())
                .setBatchSize(10);

        Config config = super.config();
        config.getCardinalityEstimatorConfig(name)
              .setBackupCount(1)
              .setAsyncBackupCount(0)
              .setMergePolicyConfig(mergePolicyConfig);
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        CardinalityEstimator estimator = instances[0].getCardinalityEstimator(name);

        for (int i = 0; i < INITIAL_COUNT; i++) {
            estimator.add(String.valueOf(i));
        }

        estimate = estimator.estimate();
        waitAllForSafeState(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        if (mergePolicyClass == DiscardMergePolicy.class) {
            onAfterSplitDiscardPolicy(firstBrain, secondBrain);
        } else if (mergePolicyClass == HyperLogLogMergePolicy.class) {
            onAfterSplitHLLMergePolicy(firstBrain, secondBrain);
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            onAfterSplitPutAbsentPolicy(firstBrain, secondBrain);
        } else {
            fail();
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        if (mergePolicyClass == DiscardMergePolicy.class) {
            onAfterMergeDiscardMergePolicy(instances);
        } else if (mergePolicyClass == HyperLogLogMergePolicy.class) {
            onAfterMergeHLLMergePolicy(instances);
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            onAfterMergePutAbsentMergePolicy(instances);
        } else {
            fail();
        }

    }

    private void onAfterSplitDiscardPolicy(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        CardinalityEstimator estimator = secondBrain[0].getCardinalityEstimator(name);
        for (int i = INITIAL_COUNT; i < TOTAL_COUNT; i++) {
            estimator.add(String.valueOf(i));
        }
    }

    private void onAfterMergeDiscardMergePolicy(HazelcastInstance[] instances) {
        CardinalityEstimator estimator = instances[0].getCardinalityEstimator(name);
        assertEquals(estimate, estimator.estimate());
    }

    private void onAfterSplitPutAbsentPolicy(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        CardinalityEstimator estimator = secondBrain[0].getCardinalityEstimator("Absent");
        for (int i = 0; i < INITIAL_COUNT; i++) {
            estimator.add(String.valueOf(i));
        }

        estimate = estimator.estimate();
    }

    private void onAfterMergePutAbsentMergePolicy(HazelcastInstance[] instances) {
        CardinalityEstimator estimator = instances[0].getCardinalityEstimator(name);
        CardinalityEstimator absent = instances[0].getCardinalityEstimator(name);

        assertEquals(estimate, estimator.estimate());
        assertEquals(estimate, absent.estimate());
    }

    private void onAfterSplitHLLMergePolicy(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        int firstBrainBump = INITIAL_COUNT + EXTRA_COUNT;

        CardinalityEstimator estimator1 = firstBrain[0].getCardinalityEstimator(name);
        for (int i = INITIAL_COUNT; i < firstBrainBump; i++) {
            estimator1.add(String.valueOf(i));
        }

        CardinalityEstimator estimator2 = secondBrain[0].getCardinalityEstimator(name);
        for (int i = INITIAL_COUNT; i < TOTAL_COUNT; i++) {
            estimator2.add(String.valueOf(i));
        }
    }

    private void onAfterMergeHLLMergePolicy(HazelcastInstance[] instances) {
        CardinalityEstimator estimator = instances[0].getCardinalityEstimator(name);
        CardinalityEstimator expected = instances[0].getCardinalityEstimator("Expected");
        for (int i = 0; i < TOTAL_COUNT; i++) {
            expected.add(String.valueOf(i));
        }

        assertEquals(expected.estimate(), estimator.estimate());
    }

}
