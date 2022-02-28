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

package com.hazelcast.query.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.query.impl.Comparables.canonicalizeForHashLookup;
import static com.hazelcast.query.impl.Comparables.compare;
import static com.hazelcast.query.impl.Comparables.equal;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ComparablesTest {

    @SuppressWarnings("ConstantConditions")
    @Test(expected = Throwable.class)
    public void testNullLhsInCompareThrows() {
        compare(null, 1);
    }

    @SuppressWarnings("ConstantConditions")
    @Test(expected = Throwable.class)
    public void testNullRhsInCompareThrows() {
        compare(1, null);
    }

    @Test(expected = Throwable.class)
    public void testIncompatibleTypesInCompare() {
        compare("string", 1);
    }

    @Test
    public void testEqual() {
        assertFalse(equal(1, null));
        assertFalse(equal(1, 2));
        assertFalse(equal(1, 1.1));
        assertFalse(equal("foo", "bar"));
        assertFalse(equal("foo", 1));
        assertFalse(equal(1.0, "foo"));
        assertFalse(equal(1.0, "1.0"));

        assertTrue(equal(1, 1));
        assertTrue(equal("foo", "foo"));
    }

    @Test
    public void testCompare() {
        assertNotEquals(0, compare(0, 1));
        assertNotEquals(0, compare("foo", "bar"));

        assertEquals(0, compare(0, 0));
        assertEquals(0, compare(1.0, 1.0));
        assertEquals(0, compare("foo", "foo"));

        assertThat(compare(0, 1), lessThan(0));
        assertThat(compare(1, 0), greaterThan(0));

        assertThat(compare("a", "b"), lessThan(0));
        assertThat(compare("b", "a"), greaterThan(0));
    }

    @Test
    public void testCanonicalization() {
        assertSame("foo", canonicalizeForHashLookup("foo"));
        assertSame(null, canonicalizeForHashLookup(null));
        assertEquals(1234L, ((Number) canonicalizeForHashLookup(1234)).longValue());
    }

}
