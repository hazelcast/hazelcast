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

package com.hazelcast.version;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.version.Version.UNKNOWN;
import static com.hazelcast.version.Version.UNKNOWN_VERSION;
import static com.hazelcast.version.Version.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class VersionTest {

    private Version V3_0 = of(3, 0);

    @Test
    public void getValue() throws Exception {
        assertEquals(3, V3_0.getMajor());
        assertEquals(0, V3_0.getMinor());
    }

    @Test
    public void isEqualTo() throws Exception {
        assertTrue(V3_0.isEqualTo(of(3, 0)));
        assertFalse(V3_0.isEqualTo(of(4, 0)));
    }

    @Test
    public void isGreaterThan() throws Exception {
        assertTrue(V3_0.isGreaterThan(of(2, 0)));
        assertFalse(V3_0.isGreaterThan(of(3, 0)));
        assertFalse(V3_0.isGreaterThan(of(4, 0)));
    }

    @Test
    public void isUnknownOrGreaterThan() throws Exception {
        assertTrue(V3_0.isUnknownOrGreaterThan(of(2, 0)));
        assertFalse(V3_0.isUnknownOrGreaterThan(of(3, 0)));
        assertFalse(V3_0.isUnknownOrGreaterThan(of(4, 0)));
        assertTrue(UNKNOWN.isUnknownOrGreaterThan(of(4, 0)));
    }

    @Test
    public void isGreaterOrEqual() throws Exception {
        assertTrue(V3_0.isGreaterOrEqual(of(2, 0)));
        assertTrue(V3_0.isGreaterOrEqual(of(3, 0)));
        assertTrue(V3_0.isGreaterOrEqual(of(3, 0)));
        assertFalse(V3_0.isGreaterOrEqual(of(4, 0)));
    }

    @Test
    public void isUnknownGreaterOrEqual() throws Exception {
        assertTrue(V3_0.isUnknownGreaterOrEqual(of(2, 0)));
        assertTrue(V3_0.isUnknownGreaterOrEqual(of(3, 0)));
        assertTrue(V3_0.isUnknownGreaterOrEqual(of(3, 0)));
        assertFalse(V3_0.isUnknownGreaterOrEqual(of(4, 0)));
        assertTrue(UNKNOWN.isUnknownGreaterOrEqual(of(4, 0)));
    }

    @Test
    public void isLessThan() throws Exception {
        assertFalse(V3_0.isLessThan(of(2, 0)));
        assertFalse(V3_0.isLessThan(of(3, 0)));
        assertTrue(V3_0.isLessThan(of(3, 1)));
        assertTrue(V3_0.isLessThan(of(4, 0)));
        assertTrue(V3_0.isLessThan(of(200, 0)));
    }

    @Test
    public void isUnknownOrLessThan() throws Exception {
        assertFalse(V3_0.isUnknownOrLessThan(of(2, 0)));
        assertFalse(V3_0.isUnknownOrLessThan(of(3, 0)));
        assertTrue(V3_0.isUnknownOrLessThan(of(3, 1)));
        assertTrue(V3_0.isUnknownOrLessThan(of(4, 0)));
        assertTrue(V3_0.isUnknownOrLessThan(of(200, 0)));
        assertTrue(UNKNOWN.isUnknownOrLessThan(of(200, 0)));
    }

    @Test
    public void isLessOrEqual() throws Exception {
        assertFalse(V3_0.isLessOrEqual(of(2, 0)));
        assertTrue(V3_0.isLessOrEqual(of(3, 0)));
        assertTrue(V3_0.isLessOrEqual(of(4, 0)));
    }

    @Test
    public void isUnknownLessOrEqual() throws Exception {
        assertFalse(V3_0.isUnknownLessOrEqual(of(2, 0)));
        assertTrue(V3_0.isUnknownLessOrEqual(of(3, 0)));
        assertTrue(V3_0.isUnknownLessOrEqual(of(4, 0)));
        assertTrue(UNKNOWN.isUnknownLessOrEqual(of(4, 0)));
    }

    @Test
    public void isBetween() throws Exception {
        assertFalse(V3_0.isBetween(of(0, 0), of(1, 0)));
        assertFalse(V3_0.isBetween(of(4, 0), of(5, 0)));

        assertTrue(V3_0.isBetween(of(3, 0), of(5, 0)));
        assertTrue(V3_0.isBetween(of(2, 0), of(3, 0)));

        assertTrue(V3_0.isBetween(of(1, 0), of(5, 0)));
    }

    @Test
    public void isUnknown() throws Exception {
        assertTrue(Version.UNKNOWN.isUnknown());
        assertTrue(Version.of(UNKNOWN_VERSION, UNKNOWN_VERSION).isUnknown());
        assertFalse(Version.of(0, -1).isUnknown());
        assertTrue(Version.of(0, 0).isUnknown());
    }

    @Test
    public void equals() throws Exception {
        assertEquals(Version.UNKNOWN, Version.UNKNOWN);
        assertEquals(Version.of(3, 0), Version.of(3, 0));

        assertFalse(Version.of(3, 0).equals(Version.of(4, 0)));
        assertFalse(Version.UNKNOWN.equals(Version.of(4, 0)));

        assertFalse(Version.UNKNOWN.equals(new Object()));
    }

    @Test
    public void hashCodeTest() throws Exception {
        assertEquals(Version.UNKNOWN.hashCode(), Version.UNKNOWN.hashCode());

        assertTrue(Version.UNKNOWN.hashCode() != Version.of(4, 0).hashCode());
    }

    @Test
    public void test_ofString() {
        Version v = of("3.0");
        assertEquals(v, V3_0);
    }
}
