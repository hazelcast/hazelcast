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

import static com.hazelcast.internal.metrics.MetricTarget.DIAGNOSTICS;
import static com.hazelcast.internal.metrics.MetricTarget.JMX;
import static com.hazelcast.internal.metrics.MetricTarget.MANAGEMENT_CENTER;
import static com.hazelcast.internal.metrics.MetricTarget.asSet;
import static com.hazelcast.test.HazelcastTestSupport.assertContainsAll;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MetricTargetTest {

    @Test
    public void testAsSet_returnsSameObjects() {
        assertTrue("set objects must be cached and reused",
                asSet(new MetricTarget[]{MANAGEMENT_CENTER}) == asSet(new MetricTarget[]{MANAGEMENT_CENTER}));
    }

    @Test
    public void testAsSet_ignoresPermutations() {
        assertEquals(
                asSet(new MetricTarget[]{MANAGEMENT_CENTER}),
                asSet(new MetricTarget[]{MANAGEMENT_CENTER, MANAGEMENT_CENTER})
        );
        assertEquals(
                asSet(new MetricTarget[]{JMX, MANAGEMENT_CENTER}),
                asSet(new MetricTarget[]{MANAGEMENT_CENTER, JMX})
        );
    }

    @Test
    public void testAsSet_supportsAllCombinations() {
        assertAsSetContainsAll(MANAGEMENT_CENTER);
        assertAsSetContainsAll(JMX);
        assertAsSetContainsAll(DIAGNOSTICS);
        assertAsSetContainsAll(DIAGNOSTICS, JMX);
        assertAsSetContainsAll(DIAGNOSTICS, MANAGEMENT_CENTER);
        assertAsSetContainsAll(MANAGEMENT_CENTER, JMX);
        assertAsSetContainsAll(MANAGEMENT_CENTER, JMX, DIAGNOSTICS);
    }

    private void assertAsSetContainsAll(MetricTarget... targets) {
        assertContainsAll(asSet(targets), asList(targets));
    }

}
