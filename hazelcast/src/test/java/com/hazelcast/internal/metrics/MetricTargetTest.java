/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.internal.metrics.MetricTarget.DIAGNOSTICS;
import static com.hazelcast.internal.metrics.MetricTarget.JMX;
import static com.hazelcast.internal.metrics.MetricTarget.MANAGEMENT_CENTER;
import static com.hazelcast.test.HazelcastTestSupport.assertContainsAll;
import static java.util.Arrays.asList;
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
        assertAsSetContainsAll();
        assertAsSetContainsAll(MANAGEMENT_CENTER);
        assertAsSetContainsAll(JMX);
        assertAsSetContainsAll(DIAGNOSTICS);
        assertAsSetContainsAll(DIAGNOSTICS, JMX);
        assertAsSetContainsAll(DIAGNOSTICS, MANAGEMENT_CENTER);
        assertAsSetContainsAll(MANAGEMENT_CENTER, JMX);
        assertAsSetContainsAll(MANAGEMENT_CENTER, JMX, DIAGNOSTICS);
    }

    private void assertAsSetContainsAll(MetricTarget... targets) {
        assertContainsAll(MetricTarget.asSet(targets), asList(targets));
    }

}
