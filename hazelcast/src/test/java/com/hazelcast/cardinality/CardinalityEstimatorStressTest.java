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

package com.hazelcast.cardinality;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class CardinalityEstimatorStressTest extends HazelcastTestSupport {

    private CardinalityEstimator estimator;

    @Before
    public void setup() {
        estimator = createHazelcastInstance().getCardinalityEstimator("StressTest_Estimator");
    }

    @Test(timeout = 16 * 60000)
    public void addBigRange_checkErrorMargin_completeWithinSixteenMins() {
        // Sparse encoding would have taken much longer to complete, thus
        // timeout helps to make sure the encoding engine used is the correct one.
        long expected = 10 * 1000 * 1000;

        float tolerancePct = .3f;
        long acceptableDelta = (long) ((tolerancePct / 100) * expected);

        int loggingFrequency = 100 * 1000;
        for (int i = 0; i < expected; i++) {
            if (i % loggingFrequency == 0) {
                System.out.println("At " + i);
            }
            estimator.add(i);
        }

        long actual = estimator.estimate();
        assertEquals(expected, actual, acceptableDelta);
    }

}
