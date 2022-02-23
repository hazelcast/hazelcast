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

package com.hazelcast.internal.util.concurrent;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BackoffIdleStrategyTest {

    @Test
    public void test_createBackoffIdleStrategy() {
        BackoffIdleStrategy idleStrategy = BackoffIdleStrategy.createBackoffIdleStrategy("foo,1,2,10,15");
        assertEquals(1, idleStrategy.yieldThreshold);
        assertEquals(3, idleStrategy.parkThreshold);
        assertEquals(10, idleStrategy.minParkPeriodNs);
        assertEquals(15, idleStrategy.maxParkPeriodNs);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_createBackoffIdleStrategy_invalidConfig() {
        BackoffIdleStrategy.createBackoffIdleStrategy("foo,1");
    }

    @Test
    public void when_proposedShiftLessThanAllowed_then_shiftProposed() {
        final BackoffIdleStrategy strat = new BackoffIdleStrategy(0, 0, 1, 4);

        assertEquals(1, strat.parkTime(0));
        assertEquals(2, strat.parkTime(1));
    }

    @Test
    public void when_maxShiftedGreaterThanMaxParkTime_thenParkMax() {
        final BackoffIdleStrategy strat = new BackoffIdleStrategy(0, 0, 3, 4);

        assertEquals(3, strat.parkTime(0));
        assertEquals(4, strat.parkTime(1));
        assertEquals(4, strat.parkTime(2));
    }

    @Test
    public void when_maxShiftedLessThanMaxParkTime_thenParkMaxShifted() {
        final BackoffIdleStrategy strat = new BackoffIdleStrategy(0, 0, 2, 3);

        assertEquals(2, strat.parkTime(0));
        assertEquals(3, strat.parkTime(1));
        assertEquals(3, strat.parkTime(2));
    }
}
