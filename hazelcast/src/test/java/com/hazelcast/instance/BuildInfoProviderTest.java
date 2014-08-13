package com.hazelcast.instance;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class BuildInfoProviderTest {

    @Test
    public void testOverrideBuildNumber() {

        System.setProperty("hazelcast.build","2");
        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();

        String version = buildInfo.getVersion();
        String build = buildInfo.getBuild();
        int buildNumber = buildInfo.getBuildNumber();

        assertEquals("3.3",version);
        assertEquals("2",build);
        assertEquals(2,buildNumber);
        assertFalse(buildInfo.isEnterprise());

    }

    @Test
    public void testReadValues() {
        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();

        String version = buildInfo.getVersion();
        String build = buildInfo.getBuild();
        int buildNumber = buildInfo.getBuildNumber();

        assertEquals("3.3",version);
        assertEquals("1",build);
        assertEquals(1,buildNumber);
        assertFalse(buildInfo.isEnterprise());

    }

    @After
    public void cleanup() {
        System.clearProperty("hazelcast.build");
    }

}
