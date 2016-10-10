package com.hazelcast.instance;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class BuildInfoProviderTest {

    // major.minor.patch-RC-SNAPSHOT
    private static final Pattern VERSION_PATTERN
            = Pattern.compile("^[\\d]+\\.[\\d]+(\\.[\\d]+)?(\\-[\\w]+)?(\\-SNAPSHOT)?$");

    @Test
    public void testPattern() {
        assertTrue(VERSION_PATTERN.matcher("3.1").matches());
        assertTrue(VERSION_PATTERN.matcher("3.1-SNAPSHOT").matches());
        assertTrue(VERSION_PATTERN.matcher("3.1-RC").matches());
        assertTrue(VERSION_PATTERN.matcher("3.1-RC1-SNAPSHOT").matches());
        assertTrue(VERSION_PATTERN.matcher("3.1.1").matches());
        assertTrue(VERSION_PATTERN.matcher("3.1.1-RC").matches());
        assertTrue(VERSION_PATTERN.matcher("3.1.1-SNAPSHOT").matches());
        assertTrue(VERSION_PATTERN.matcher("3.1.1-RC1-SNAPSHOT").matches());

        assertFalse(VERSION_PATTERN.matcher("${project.version}").matches());
        assertFalse(VERSION_PATTERN.matcher("project.version").matches());
        assertFalse(VERSION_PATTERN.matcher("3").matches());
        assertFalse(VERSION_PATTERN.matcher("3.RC").matches());
        assertFalse(VERSION_PATTERN.matcher("3.SNAPSHOT").matches());
        assertFalse(VERSION_PATTERN.matcher("3-RC").matches());
        assertFalse(VERSION_PATTERN.matcher("3-SNAPSHOT").matches());
        assertFalse(VERSION_PATTERN.matcher("3.").matches());
        assertFalse(VERSION_PATTERN.matcher("3.1.RC").matches());
        assertFalse(VERSION_PATTERN.matcher("3.1.SNAPSHOT").matches());
    }

    @Test
    public void testOverrideBuildNumber() {
        System.setProperty("hazelcast.build", "2");
        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();

        String version = buildInfo.getVersion();
        String build = buildInfo.getBuild();
        int buildNumber = buildInfo.getBuildNumber();

        assertTrue(buildInfo.toString(), VERSION_PATTERN.matcher(version).matches());
        assertEquals("2", build);
        assertEquals(2, buildNumber);
        assertFalse(buildInfo.toString(), buildInfo.isEnterprise());
    }

    @Test
    public void testReadValues() {
        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();

        String version = buildInfo.getVersion();
        String build = buildInfo.getBuild();
        int buildNumber = buildInfo.getBuildNumber();

        assertTrue(buildInfo.toString(), VERSION_PATTERN.matcher(version).matches());
        assertEquals(buildInfo.toString(), buildNumber, Integer.parseInt(build));
        assertFalse(buildInfo.toString(), buildInfo.isEnterprise());

    }

    @Test
    public void testCalculateVersion() {
        assertEquals(-1, BuildInfo.calculateVersion(null));
        assertEquals(-1, BuildInfo.calculateVersion(""));
        assertEquals(-1, BuildInfo.calculateVersion("a.3.7.5"));
        assertEquals(-1, BuildInfo.calculateVersion("3.a.5"));
        assertEquals(-1, BuildInfo.calculateVersion("3,7.5"));
        assertEquals(-1, BuildInfo.calculateVersion("3.7,5"));
        assertEquals(-1, BuildInfo.calculateVersion("10.99.RC1"));

        assertEquals(30700, BuildInfo.calculateVersion("3.7-SNAPSHOT"));
        assertEquals(30702, BuildInfo.calculateVersion("3.7.2"));
        assertEquals(30702, BuildInfo.calculateVersion("3.7.2-SNAPSHOT"));
        assertEquals(109902, BuildInfo.calculateVersion("10.99.2-SNAPSHOT"));
        assertEquals(19930, BuildInfo.calculateVersion("1.99.30"));
        assertEquals(109930, BuildInfo.calculateVersion("10.99.30-SNAPSHOT"));
        assertEquals(109900, BuildInfo.calculateVersion("10.99-RC1"));
    }

    @After
    public void cleanup() {
        System.clearProperty("hazelcast.build");
    }

}
