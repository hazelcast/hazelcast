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
