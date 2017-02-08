package com.hazelcast.internal.cluster.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class VersionsTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(Versions.class);
    }

    @Test
    public void version_3_8() {
        assertEquals(Version.of(3, 8), Versions.V3_8);
    }

    @Test
    public void version_3_9() {
        assertEquals(Version.of(3, 9), Versions.V3_9);
    }
}
