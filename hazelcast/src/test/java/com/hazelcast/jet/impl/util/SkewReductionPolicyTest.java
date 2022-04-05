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

package com.hazelcast.jet.impl.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SkewReductionPolicyTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private SkewReductionPolicy srp;

    @Test
    public void when_skewedWm_then_drainOrderCorrect() {
        srp = new SkewReductionPolicy(4, 1000, 500, false);

        srp.observeWm(1, 2);
        assertQueuesOrdered();
        srp.observeWm(2, 0);
        assertQueuesOrdered();
        srp.observeWm(3, 3);
        assertQueuesOrdered();
        srp.observeWm(0, 4);
        assertQueuesOrdered();
        // the most advanced becomes even more advanced
        srp.observeWm(0, 5);
        assertQueuesOrdered();
        // the least advanced advances, but still the least advanced
        srp.observeWm(2, 1);
        assertQueuesOrdered();
        // all queue wms become equal
        for (int i = 0; i < srp.drainOrderToQIdx.length; i++) {
            srp.observeWm(i, 6);
            assertQueuesOrdered();
        }
    }

    @Test
    public void when_maxSkewIsMaxVal_and_forceAdvancing_then_correctnessMaintained() {
        // Given
        long maxSkew = Long.MAX_VALUE;
        srp = new SkewReductionPolicy(2, maxSkew, 10, true);
        long[] wms = srp.queueWms;

        // When
        srp.observeWm(0, 10);

        // Then
        assertEquals(maxSkew, wms[0] - wms[1]);
        assertFalse(srp.shouldStopDraining(0, false));
    }

    @Test
    public void when_maxSkewIsMaxVal_and_notForceAdvancing_then_correctnessMaintained() {
        // Given
        srp = new SkewReductionPolicy(2, Long.MAX_VALUE, 10, false);
        long[] wms = srp.queueWms;

        // When
        srp.observeWm(0, 10);

        // Then
        assertEquals(Long.MIN_VALUE, wms[1]);
        assertFalse(srp.shouldStopDraining(0, false));
    }

    @Test
    public void when_maxSkewAlmostMaxVal_and_notForceAdvancing_then_correctnessMaintained() {
        // Given
        srp = new SkewReductionPolicy(2, Long.MAX_VALUE - 1, 10, false);
        long[] wms = srp.queueWms;

        // When
        srp.observeWm(0, 10);

        // Then
        assertEquals(Long.MIN_VALUE, wms[1]);
        assertTrue(srp.shouldStopDraining(0, false));
    }

    @Test
    public void when_maxSkewAlmostMaxVal_and_forceAdvancing_then_correctnessMaintained() {
        // Given
        long maxSkew = Long.MAX_VALUE - 1;
        srp = new SkewReductionPolicy(2, maxSkew, 10, true);
        long[] wms = srp.queueWms;

        // When
        srp.observeWm(0, 10);

        // Then
        assertEquals(maxSkew, wms[0] - wms[1]);
        assertFalse(srp.shouldStopDraining(0, false));
    }

    @Test
    public void when_priorityThresholdIsMaxVal_then_correctnessMaintained() {
        // Given
        srp = new SkewReductionPolicy(2, Long.MAX_VALUE, Long.MAX_VALUE, false);
        long[] wms = srp.queueWms;

        // When
        srp.observeWm(0, 10);

        // Then
        assertEquals(Long.MIN_VALUE, wms[1]);
        assertFalse(srp.shouldStopDraining(0, true));
    }

    @Test
    public void when_skewBeyondMaxVal_then_correctnessMaintained() {
        // Given
        srp = new SkewReductionPolicy(2, 20, 10, false);
        long[] wms = srp.queueWms;

        // When
        srp.observeWm(0, 10);

        // Then
        assertEquals(Long.MIN_VALUE, wms[1]);
        assertTrue(srp.shouldStopDraining(0, true));
    }


    private void assertQueuesOrdered() {
        long lastValue = Long.MIN_VALUE;
        for (int i = 1; i < srp.queueWms.length; i++) {
            long thisValue = srp.queueWms[srp.drainOrderToQIdx[i]];
            assertTrue("Queues not ordered"
                            + "\nobservedWmSeqs=" + Arrays.toString(srp.queueWms)
                            + "\norderedQueues=" + Arrays.toString(srp.drainOrderToQIdx),
                    lastValue <= thisValue);
            lastValue = thisValue;
        }
        Set<Integer> set = new HashSet<>();
        for (int i : srp.drainOrderToQIdx) {
            set.add(i);
        }
        assertEquals(srp.drainOrderToQIdx.length, set.size());
    }
}
