/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CardinalityEstimatorSplitBrainTest
        extends SplitBrainTestSupport {

    private final String name = "Actual";

    private final int initialCount = 100000;
    private final int extraCount = 10000;
    private final int numOfBrains = 2;
    private final int totalCount = initialCount + (numOfBrains * extraCount);

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        CardinalityEstimator estimator = instances[0].getCardinalityEstimator(name);

        for (int i = 0; i < initialCount; i++) {
            estimator.add(String.valueOf(i));
        }

        waitAllForSafeState(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        int base = initialCount;
        int firstBrainBump = initialCount + extraCount;

        CardinalityEstimator estimator1 = firstBrain[0].getCardinalityEstimator(name);
        for (int i = base; i < firstBrainBump; i++) {
            estimator1.add(String.valueOf(i));
        }

        base = firstBrainBump;
        CardinalityEstimator estimator2 = secondBrain[0].getCardinalityEstimator(name);
        for (int i = base; i < totalCount; i++) {
            estimator2.add(String.valueOf(i));
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        CardinalityEstimator estimator = instances[0].getCardinalityEstimator(name);
        CardinalityEstimator expected = instances[0].getCardinalityEstimator("Expected");
        for (int i = 0; i < totalCount; i++) {
            expected.add(String.valueOf(i));
        }

        assertEquals(expected.estimate(), estimator.estimate());
    }

}
