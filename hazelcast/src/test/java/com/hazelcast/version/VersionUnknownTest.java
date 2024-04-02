/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.version;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.version.Version.UNKNOWN;
import static com.hazelcast.version.Version.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class VersionUnknownTest {

    private Version ANY_VERSION = of(3, 7);

    @Test
    public void unknown_equals_to_itself() {
        assertEquals(UNKNOWN, UNKNOWN);
    }

    @Test
    public void unknown_notEquals_to_any() {
        assertNotEquals(UNKNOWN, ANY_VERSION);
    }

    @Test
    public void unknown_isNot_greaterThan_any() {
        assertFalse(UNKNOWN.isGreaterThan(ANY_VERSION));
    }

    @Test
    public void unknown_isNot_greaterThan_unknown() {
        assertFalse(UNKNOWN.isGreaterThan(UNKNOWN));
    }

    @Test
    public void unknown_isNot_greaterOrEqual_any() {
        assertFalse(UNKNOWN.isGreaterOrEqual(ANY_VERSION));
    }

    @Test
    public void unknown_is_greaterOrEqual_unknown() {
        assertTrue(UNKNOWN.isGreaterOrEqual(UNKNOWN));
    }

    @Test
    public void unknown_isNot_lessThan_any() {
        assertFalse(UNKNOWN.isLessThan(ANY_VERSION));
    }

    @Test
    public void unknown_isNot_lessThan_unknown() {
        assertFalse(UNKNOWN.isLessThan(UNKNOWN));
    }

    @Test
    public void unknown_isNot_lessOrEqual_any() {
        assertFalse(UNKNOWN.isLessOrEqual(ANY_VERSION));
    }

    @Test
    public void unknown_is_lessOrEqual_unknown() {
        assertTrue(UNKNOWN.isLessOrEqual(UNKNOWN));
    }

    @Test
    public void unknown_is_unknownOrGreaterThan_any() {
        assertTrue(UNKNOWN.isUnknownOrGreaterThan(ANY_VERSION));
    }

    @Test
    public void unknown_is_unknownOrGreaterThan_unknown() {
        assertTrue(UNKNOWN.isUnknownOrGreaterThan(UNKNOWN));
    }

    @Test
    public void unknown_is_unknownOrLessThan_any() {
        assertTrue(UNKNOWN.isUnknownOrLessThan(ANY_VERSION));
    }

    @Test
    public void unknown_is_unknownOrLessThan_unknown() {
        assertTrue(UNKNOWN.isUnknownOrLessThan(UNKNOWN));
    }

    @Test
    public void unknown_is_unknownGreaterOrEqual_any() {
        assertTrue(UNKNOWN.isUnknownOrGreaterOrEqual(ANY_VERSION));
    }

    @Test
    public void unknown_is_unknownGreaterOrEqual_unknown() {
        assertTrue(UNKNOWN.isUnknownOrGreaterOrEqual(UNKNOWN));
    }

    @Test
    public void unknown_is_unknownLessOrEqual_any() {
        assertTrue(UNKNOWN.isUnknownOrLessOrEqual(ANY_VERSION));
    }

    @Test
    public void unknown_is_unknownLessOrEqual_unknown() {
        assertTrue(UNKNOWN.isUnknownOrLessOrEqual(UNKNOWN));
    }

    @Test
    public void any_notEquals_to_unknown() {
        assertNotEquals(ANY_VERSION, UNKNOWN);
    }

    @Test
    public void any_isNot_greaterThan_unknown() {
        assertFalse(ANY_VERSION.isGreaterThan(UNKNOWN));
    }

    @Test
    public void any_isNot_greaterOrEqual_unknown() {
        assertFalse(ANY_VERSION.isGreaterOrEqual(UNKNOWN));
    }

    @Test
    public void any_isNot_lessThan_unknown() {
        assertFalse(ANY_VERSION.isLessThan(UNKNOWN));
    }

    @Test
    public void any_isNot_lessOrEqual_unknown() {
        assertFalse(ANY_VERSION.isLessOrEqual(UNKNOWN));
    }

    @Test
    public void any_isNot_unknownOrGreaterThan_unknown() {
        assertFalse(ANY_VERSION.isUnknownOrGreaterThan(UNKNOWN));
    }

    @Test
    public void any_isNot_unknownOrLessThan_unknown() {
        assertFalse(ANY_VERSION.isUnknownOrLessThan(UNKNOWN));
    }

    @Test
    public void any_isNot_unknownGreaterOrEqual_unknown() {
        assertFalse(ANY_VERSION.isUnknownOrGreaterOrEqual(UNKNOWN));
    }

    @Test
    public void any_isNot_unknownLessOrEqual_unknown() {
        assertFalse(ANY_VERSION.isUnknownOrLessOrEqual(UNKNOWN));
    }

}
