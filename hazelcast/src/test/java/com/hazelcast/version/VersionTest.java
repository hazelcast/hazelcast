package com.hazelcast.version;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.version.Version.MAJOR_MINOR_VERSION_COMPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class VersionTest {

    static final String VERSION_3_8_SNAPSHOT_STRING = "3.8-SNAPSHOT";
    static final String VERSION_3_8_1_RC1_STRING = "3.8.1-RC1";
    static final String VERSION_3_8_2_STRING = "3.8.2";
    static final String VERSION_3_9_0_STRING = "3.9.0";

    static final Version VERSION_3_8 = Version.of(VERSION_3_8_SNAPSHOT_STRING);
    static final Version VERSION_3_8_1 = Version.of(VERSION_3_8_1_RC1_STRING);
    static final Version VERSION_3_8_2 = Version.of(VERSION_3_8_2_STRING);
    static final Version VERSION_3_9 = Version.of(VERSION_3_9_0_STRING);

    @Test
    public void test_versionOf_whenVersionStringIsSnapshot() {
        Version expected = Version.of(3, 8, 0);
        assertEquals(expected, Version.of(VERSION_3_8_SNAPSHOT_STRING));
    }

    @Test
    public void test_versionOf_whenVersionStringIsRC() {
        Version expected = Version.of(3, 8, 1);
        assertEquals(expected, Version.of(VERSION_3_8_1_RC1_STRING));
    }

    @Test
    public void test_versionOf_whenVersionStringIsRelease() {
        Version expected = Version.of(3, 8, 2);
        assertEquals(expected, Version.of(VERSION_3_8_2_STRING));
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