package com.hazelcast.nio;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.nio.Version.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})

public class VersionTest {

    private Version v3 = of(3);

    @Test
    public void getValue() throws Exception {
        assertEquals(3, v3.getValue());
    }

    @Test
    public void isEqualTo() throws Exception {
        assertTrue(v3.isEqualTo(of(3)));
        assertFalse(v3.isEqualTo(of(4)));
    }

    @Test
    public void isGreaterThan() throws Exception {
        assertTrue(v3.isGreaterThan(of(2)));
        assertFalse(v3.isGreaterThan(of(3)));
        assertFalse(v3.isGreaterThan(of(4)));
    }

    @Test
    public void isGreaterOrEqual() throws Exception {
        assertTrue(v3.isGreaterOrEqual(of(2)));
        assertTrue(v3.isGreaterOrEqual(of(3)));
        assertFalse(v3.isGreaterOrEqual(of(4)));
    }

    @Test
    public void isLessThan() throws Exception {
        assertFalse(v3.isLessThan(of(2)));
        assertFalse(v3.isLessThan(of(3)));
        assertTrue(v3.isLessThan(of(4)));
    }

    @Test
    public void isLessOrEqual() throws Exception {
        assertFalse(v3.isLessOrEqual(of(2)));
        assertTrue(v3.isLessOrEqual(of(3)));
        assertTrue(v3.isLessOrEqual(of(4)));
    }

    @Test
    public void isBetween() throws Exception {
        assertFalse(v3.isBetween(of(0), of(1)));
        assertFalse(v3.isBetween(of(4), of(5)));

        assertTrue(v3.isBetween(of(3), of(5)));
        assertTrue(v3.isBetween(of(2), of(3)));

        assertTrue(v3.isBetween(of(1), of(5)));
    }

    @Test
    public void isUnknown() throws Exception {
        assertTrue(Version.UNKNOWN.isUnknown());
        assertTrue(Version.of(Version.UNKNOWN_VERSION).isUnknown());
        assertTrue(Version.of(-1).isUnknown());
        assertFalse(Version.of(0).isUnknown());
    }


    @Test
    public void equals() throws Exception {
        assertEquals(Version.UNKNOWN, Version.UNKNOWN);
        assertEquals(Version.of(3), Version.of(3));

        assertFalse(Version.of(3).equals(Version.of(4)));
        assertFalse(Version.UNKNOWN.equals(Version.of(4)));

        assertFalse(Version.UNKNOWN.equals(new Object()));
    }

    @Test
    public void hashCodeTest() throws Exception {
        assertEquals(Version.UNKNOWN.hashCode(), Version.UNKNOWN.hashCode());

        assertTrue(Version.UNKNOWN.hashCode() != Version.of(4).hashCode());
    }

}