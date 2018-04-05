/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.util.JavaVersion.JAVA_1_6;
import static com.hazelcast.internal.util.JavaVersion.JAVA_1_7;
import static com.hazelcast.internal.util.JavaVersion.JAVA_1_8;
import static com.hazelcast.internal.util.JavaVersion.JAVA_1_9;
import static com.hazelcast.internal.util.JavaVersion.UNKNOWN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class JavaVersionTest extends HazelcastTestSupport {

    @Test
    public void parseVersion() throws Exception {
        assertEquals(UNKNOWN, JavaVersion.parseVersion("foo"));
        assertEquals(JAVA_1_6, JavaVersion.parseVersion("1.6"));
        assertEquals(JAVA_1_7, JavaVersion.parseVersion("1.7"));
        assertEquals(JAVA_1_8, JavaVersion.parseVersion("1.8"));
        assertEquals(JAVA_1_9, JavaVersion.parseVersion("9-ea"));
        assertEquals(JAVA_1_9, JavaVersion.parseVersion("9"));
    }

    @Test
    public void testIsAtLeast_unknown() {
        assertTrue(JavaVersion.isAtLeast(UNKNOWN, UNKNOWN));
        assertFalse(JavaVersion.isAtLeast(UNKNOWN, JAVA_1_6));
        assertFalse(JavaVersion.isAtLeast(UNKNOWN, JAVA_1_7));
        assertFalse(JavaVersion.isAtLeast(UNKNOWN, JAVA_1_8));
        assertFalse(JavaVersion.isAtLeast(UNKNOWN, JAVA_1_9));
    }

    @Test
    public void testIsAtLeast_1_6() {
        assertTrue(JavaVersion.isAtLeast(JAVA_1_6, UNKNOWN));
        assertTrue(JavaVersion.isAtLeast(JAVA_1_6, JAVA_1_6));
        assertFalse(JavaVersion.isAtLeast(JAVA_1_6, JAVA_1_7));
        assertFalse(JavaVersion.isAtLeast(JAVA_1_6, JAVA_1_8));
        assertFalse(JavaVersion.isAtLeast(JAVA_1_6, JAVA_1_9));
    }

    @Test
    public void testIsAtLeast_1_7() {
        assertTrue(JavaVersion.isAtLeast(JAVA_1_7, UNKNOWN));
        assertTrue(JavaVersion.isAtLeast(JAVA_1_7, JAVA_1_6));
        assertTrue(JavaVersion.isAtLeast(JAVA_1_7, JAVA_1_7));
        assertFalse(JavaVersion.isAtLeast(JAVA_1_7, JAVA_1_8));
        assertFalse(JavaVersion.isAtLeast(JAVA_1_7, JAVA_1_9));
    }

    @Test
    public void testIsAtLeast_1_8() {
        assertTrue(JavaVersion.isAtLeast(JAVA_1_8, UNKNOWN));
        assertTrue(JavaVersion.isAtLeast(JAVA_1_8, JAVA_1_6));
        assertTrue(JavaVersion.isAtLeast(JAVA_1_8, JAVA_1_7));
        assertTrue(JavaVersion.isAtLeast(JAVA_1_8, JAVA_1_8));
        assertFalse(JavaVersion.isAtLeast(JAVA_1_8, JAVA_1_9));
    }

    @Test
    public void testIsAtLeast_1_9() {
        assertTrue(JavaVersion.isAtLeast(JAVA_1_9, UNKNOWN));
        assertTrue(JavaVersion.isAtLeast(JAVA_1_9, JAVA_1_6));
        assertTrue(JavaVersion.isAtLeast(JAVA_1_9, JAVA_1_7));
        assertTrue(JavaVersion.isAtLeast(JAVA_1_9, JAVA_1_8));
        assertTrue(JavaVersion.isAtLeast(JAVA_1_9, JAVA_1_9));
    }
}
