package com.hazelcast.util;

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
public class HazelcastUtilTest {


    @Test
    public void testOverrideBuildNumber() {

        System.setProperty("hazelcast.build","2");
        HazelcastUtil.init();

        String version = HazelcastUtil.getVersion();
        String build = HazelcastUtil.getBuild();
        int buildNumber = HazelcastUtil.getBuildNumber();

        assertEquals("3.3",version);
        assertEquals("2",build);
        assertEquals(2,buildNumber);
        assertFalse(HazelcastUtil.isEnterprise());

    }

    @Test
    public void testReadValues() {

        String version = HazelcastUtil.getVersion();
        String build = HazelcastUtil.getBuild();
        int buildNumber = HazelcastUtil.getBuildNumber();

        assertEquals("3.3",version);
        assertEquals("1",build);
        assertEquals(1,buildNumber);
        assertFalse(HazelcastUtil.isEnterprise());

    }

    @After
    public void cleanup() {
        System.clearProperty("hazelcast.build");
    }

}
