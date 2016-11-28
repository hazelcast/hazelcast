package com.hazelcast.version;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.version.MemberVersion.MAJOR_MINOR_VERSION_COMPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberVersionTest {

    private static final String VERSION_3_8_SNAPSHOT_STRING = "3.8-SNAPSHOT";
    private static final String VERSION_3_8_1_RC1_STRING = "3.8.1-RC1";
    private static final String VERSION_3_8_2_STRING = "3.8.2";
    private static final String VERSION_3_9_0_STRING = "3.9.0";
    private static final String VERSION_UNKNOWN_STRING = "0.0.0";

    private static final MemberVersion VERSION_3_8 = MemberVersion.of(VERSION_3_8_SNAPSHOT_STRING);
    private static final MemberVersion VERSION_3_8_1 = MemberVersion.of(VERSION_3_8_1_RC1_STRING);
    private static final MemberVersion VERSION_3_8_2 = MemberVersion.of(VERSION_3_8_2_STRING);
    private static final MemberVersion VERSION_3_9 = MemberVersion.of(VERSION_3_9_0_STRING);

    private MemberVersion version = MemberVersion.of(3, 8, 0);
    private MemberVersion versionSameAttributes = MemberVersion.of(3, 8, 0);
    private MemberVersion versionOtherMajor = MemberVersion.of(4, 8, 0);
    private MemberVersion versionOtherMinor = MemberVersion.of(3, 7, 0);
    private MemberVersion versionOtherPath = MemberVersion.of(3, 8, 1);

    @Test
    public void testIsUnknown() {
        assertTrue(MemberVersion.UNKNOWN.isUnknown());
        assertFalse(MemberVersion.of(VERSION_3_8_SNAPSHOT_STRING).isUnknown());
        assertFalse(MemberVersion.of(VERSION_3_8_1_RC1_STRING).isUnknown());
        assertFalse(MemberVersion.of(VERSION_3_8_2_STRING).isUnknown());
    }

    @Test
    public void testVersionOf_whenVersionIsUnknown() {
        assertEquals(MemberVersion.UNKNOWN, MemberVersion.of(0, 0, 0));
    }

    @Test
    public void testVersionOf_whenVersionStringIsSnapshot() {
        MemberVersion expected = MemberVersion.of(3, 8, 0);
        assertEquals(expected, MemberVersion.of(VERSION_3_8_SNAPSHOT_STRING));
    }

    @Test
    public void testVersionOf_whenVersionStringIsRC() {
        MemberVersion expected = MemberVersion.of(3, 8, 1);
        assertEquals(expected, MemberVersion.of(VERSION_3_8_1_RC1_STRING));
    }

    @Test
    public void testVersionOf_whenVersionStringIsRelease() {
        MemberVersion expected = MemberVersion.of(3, 8, 2);
        assertEquals(expected, MemberVersion.of(VERSION_3_8_2_STRING));
    }

    @Test
    public void testVersionOf_whenVersionStringIsUnknown() {
        assertEquals(MemberVersion.UNKNOWN, MemberVersion.of(VERSION_UNKNOWN_STRING));
    }

    @Test
    public void testVersionOf_whenVersionStringIsNull() {
        assertEquals(MemberVersion.UNKNOWN, MemberVersion.of(null));
    }

    @Test
    public void testEquals() {
        assertEquals(version, version);
        assertEquals(version, versionSameAttributes);

        assertNotEquals(version, null);
        assertNotEquals(version, new Object());

        assertNotEquals(version, versionOtherMajor);
        assertNotEquals(version, versionOtherMinor);
        assertNotEquals(version, versionOtherPath);
    }

    @Test
    public void testHashCode() {
        assertEquals(version.hashCode(), version.hashCode());
        assertEquals(version.hashCode(), versionSameAttributes.hashCode());

        assertNotEquals(version.hashCode(), versionOtherMajor.hashCode());
        assertNotEquals(version.hashCode(), versionOtherMinor.hashCode());
        assertNotEquals(version.hashCode(), versionOtherPath.hashCode());
    }

    @Test
    public void testCompareTo() {
        assertTrue(VERSION_3_8.compareTo(VERSION_3_8) == 0);
        assertTrue(VERSION_3_8.compareTo(VERSION_3_8_1) < 0);
        assertTrue(VERSION_3_8.compareTo(VERSION_3_8_2) < 0);
        assertTrue(VERSION_3_8.compareTo(VERSION_3_9) < 0);

        assertTrue(VERSION_3_9.compareTo(VERSION_3_8) > 0);
        assertTrue(VERSION_3_9.compareTo(VERSION_3_8_1) > 0);
        assertTrue(VERSION_3_9.compareTo(VERSION_3_8_2) > 0);
    }

    @Test
    public void testMajorMinorVersionComparator() {
        assertEquals(0, MAJOR_MINOR_VERSION_COMPARATOR.compare(VERSION_3_8, VERSION_3_8_1));
        assertEquals(0, MAJOR_MINOR_VERSION_COMPARATOR.compare(VERSION_3_8, VERSION_3_8_2));
        assertTrue(MAJOR_MINOR_VERSION_COMPARATOR.compare(VERSION_3_9, VERSION_3_8) > 0);
        assertTrue(MAJOR_MINOR_VERSION_COMPARATOR.compare(VERSION_3_8, VERSION_3_9) < 0);
        assertTrue(MAJOR_MINOR_VERSION_COMPARATOR.compare(VERSION_3_9, VERSION_3_8_1) > 0);
        assertTrue(MAJOR_MINOR_VERSION_COMPARATOR.compare(VERSION_3_8_1, VERSION_3_9) < 0);
    }
}
