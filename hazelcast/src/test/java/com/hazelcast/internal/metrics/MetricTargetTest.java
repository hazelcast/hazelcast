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

package com.hazelcast.internal.metrics;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

import static com.google.common.math.IntMath.factorial;
import static com.hazelcast.internal.metrics.MetricTarget.DIAGNOSTICS;
import static com.hazelcast.internal.metrics.MetricTarget.JMX;
import static com.hazelcast.internal.metrics.MetricTarget.MANAGEMENT_CENTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MetricTargetTest {

    @Test
    public void testAsSet_returnsSameObjects() {
        assertSame(
                MetricTarget.asSet(new MetricTarget[]{MANAGEMENT_CENTER}),
                MetricTarget.asSet(new MetricTarget[]{MANAGEMENT_CENTER})
        );
    }

    @Test
    public void testAsSet_ignoresPermutations() {
        assertSame(
                MetricTarget.asSet(new MetricTarget[]{MANAGEMENT_CENTER}),
                MetricTarget.asSet(new MetricTarget[]{MANAGEMENT_CENTER, MANAGEMENT_CENTER})
        );
        assertSame(
                MetricTarget.asSet(new MetricTarget[]{JMX, MANAGEMENT_CENTER}),
                MetricTarget.asSet(new MetricTarget[]{MANAGEMENT_CENTER, JMX})
        );
    }

    @Test
    public void testAsSetWith_returnsSameObjects() {
        Set<MetricTarget> targetsWithoutJmx = MetricTarget.asSet(new MetricTarget[]{MANAGEMENT_CENTER});
        Set<MetricTarget> targetsWithJmx = MetricTarget.asSet(new MetricTarget[]{MANAGEMENT_CENTER, JMX});
        assertSame(
                targetsWithJmx,
                MetricTarget.asSetWith(targetsWithoutJmx, JMX)
        );
    }

    @Test
    public void testAsSetWithout_returnsSameObjects() {
        Set<MetricTarget> targetsWithoutJmx = MetricTarget.asSet(new MetricTarget[]{MANAGEMENT_CENTER});
        Set<MetricTarget> targetsWithJmx = MetricTarget.asSet(new MetricTarget[]{MANAGEMENT_CENTER, JMX});
        assertSame(
                targetsWithoutJmx,
                MetricTarget.asSetWithout(targetsWithJmx, JMX)
        );
    }

    @Test
    @SuppressWarnings("checkstyle:IllegalTokenText")
    public void testBitset() {
        Set<MetricTarget> targets = MetricTarget.asSet(new MetricTarget[]{MANAGEMENT_CENTER, DIAGNOSTICS});
        assertEquals(0b101, MetricTarget.bitset(targets));
    }

    @Test
    @SuppressWarnings("checkstyle:IllegalTokenText")
    public void testAsSet_bitset() {
        Set<MetricTarget> expectedTargets = MetricTarget.asSet(new MetricTarget[]{MANAGEMENT_CENTER, DIAGNOSTICS});
        assertSame(
                expectedTargets,
                MetricTarget.asSet(0b101)
        );
    }

    @Test
    public void testAsSet_supportsAllCombinations() {
        int all = MetricTarget.values().length;
        int allCombinations = 0;
        for (int choose = 0; choose <= all; choose++) {
            // C(n,r) = n!/r!(n-r)!
            allCombinations += factorial(all) / (factorial(choose) * factorial(all - choose));
        }

        assertEquals(allCombinations, MetricTarget.BITSET_TO_SET_CACHE.size());
    }

    @Test
    public void testUnion() {
        Set<MetricTarget> excludedTargets1 = MetricTarget.asSet(MANAGEMENT_CENTER, JMX);
        Set<MetricTarget> excludedTargets2 = MetricTarget.asSet(DIAGNOSTICS, JMX);

        assertSame(
            MetricTarget.asSet(MANAGEMENT_CENTER, DIAGNOSTICS, JMX),
            MetricTarget.union(excludedTargets1, excludedTargets2)
        );
    }
}
