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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BasicBoundedRangePredicateTest {

    @Test(expected = IllegalArgumentException.class)
    public void testLeftBoundedRange() {
        new BoundedRangePredicate("this", 1, true, null, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRightBoundedRange() {
        new BoundedRangePredicate("this", null, true, 10, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnboundedRange() {
        new BoundedRangePredicate("this", null, true, null, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCantBeSerialized() {
        new BoundedRangePredicate("this", 1, true, 10, true).getClassId();
    }

    @Test
    public void testRangePredicateConformance() {
        assertRangePredicate(new BoundedRangePredicate("this", 1, true, 10, true), "this", 1, true, 10, true);
        assertRangePredicate(new BoundedRangePredicate("__key", 11, false, 20, true), "__key", 11, false, 20, true);
        assertRangePredicate(new BoundedRangePredicate("this", 21, true, 30, false), "this", 21, true, 30, false);
        assertRangePredicate(new BoundedRangePredicate("__key", 31, false, 40, false), "__key", 31, false, 40, false);
    }

    private void assertRangePredicate(RangePredicate actual, String expectedAttribute, Comparable expectedFrom,
                                      boolean expectedFromInclusive, Comparable expectedTo, boolean expectedToInclusive) {
        assertEquals(expectedAttribute, actual.getAttribute());
        assertEquals(expectedFrom, actual.getFrom());
        assertEquals(expectedFromInclusive, actual.isFromInclusive());
        assertEquals(expectedTo, actual.getTo());
        assertEquals(expectedToInclusive, actual.isToInclusive());
    }

}
