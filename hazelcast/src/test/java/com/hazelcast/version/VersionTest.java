package com.hazelcast.version;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.version.Version.MAJOR_MINOR_VERSION_COMPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class VersionTest {

    private static final String VERSION_3_8_SNAPSHOT_STRING = "3.8-SNAPSHOT";
    private static final String VERSION_3_8_1_RC1_STRING = "3.8.1-RC1";
    private static final String VERSION_3_8_2_STRING = "3.8.2";
    private static final String VERSION_3_9_0_STRING = "3.9.0";
    private static final String VERSION_UNKNOWN_STRING = "0.0.0";

    private static final Version VERSION_3_8 = Version.of(VERSION_3_8_SNAPSHOT_STRING);
    private static final Version VERSION_3_8_1 = Version.of(VERSION_3_8_1_RC1_STRING);
    private static final Version VERSION_3_8_2 = Version.of(VERSION_3_8_2_STRING);
    private static final Version VERSION_3_9 = Version.of(VERSION_3_9_0_STRING);

    private Version version = Version.of(3, 8, 0);
    private Version versionSameAttributes = Version.of(3, 8, 0);
    private Version versionOtherMajor = Version.of(4, 8, 0);
    private Version versionOtherMinor = Version.of(3, 7, 0);
    private Version versionOtherPath = Version.of(3, 8, 1);

    @Test
    public void testIsUnknown() {
        assertTrue(Version.UNKNOWN.isUnknown());
        assertFalse(Version.of(VERSION_3_8_SNAPSHOT_STRING).isUnknown());
        assertFalse(Version.of(VERSION_3_8_1_RC1_STRING).isUnknown());
        assertFalse(Version.of(VERSION_3_8_2_STRING).isUnknown());
    }

    @Test
    public void testVersionOf_whenVersionIsUnknown() {
        assertEquals(Version.UNKNOWN, Version.of(0, 0, 0));
    }

    @Test
    public void testVersionOf_whenVersionStringIsSnapshot() {
        Version expected = Version.of(3, 8, 0);
        assertEquals(expected, Version.of(VERSION_3_8_SNAPSHOT_STRING));
    }

    @Test
    public void testVersionOf_whenVersionStringIsRC() {
        Version expected = Version.of(3, 8, 1);
        assertEquals(expected, Version.of(VERSION_3_8_1_RC1_STRING));
    }

    @Test
    public void testVersionOf_whenVersionStringIsRelease() {
        Version expected = Version.of(3, 8, 2);
        assertEquals(expected, Version.of(VERSION_3_8_2_STRING));
    }

    @Test
    public void testVersionOf_whenVersionStringIsUnknown() {
        assertEquals(Version.UNKNOWN, Version.of(VERSION_UNKNOWN_STRING));
    }

    @Test
    public void testVersionOf_whenVersionStringIsNull() {
        assertEquals(Version.UNKNOWN, Version.of(null));
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
