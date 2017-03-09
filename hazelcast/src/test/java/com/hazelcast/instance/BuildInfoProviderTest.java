/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;
import java.util.regex.Pattern;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class BuildInfoProviderTest extends HazelcastTestSupport {

    // major.minor.patch-RC-SNAPSHOT
    private static final Pattern VERSION_PATTERN = Pattern.compile("^[\\d]+\\.[\\d]+(\\.[\\d]+)?(\\-[\\w]+)?(\\-SNAPSHOT)?$");

    @After
    public void cleanup() {
        System.clearProperty("hazelcast.build");
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(BuildInfoProvider.class);
    }

    @Test
    public void testVersionPattern() {
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

    @Test
    public void testOverrideBuildVersion() {
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, "99.99.99");
        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
        assertEquals("99.99.99", buildInfo.getVersion());
        System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
    }

    @Test
    public void testJetBuildInfo() {
        Properties properties = new Properties();
        properties.setProperty("jet.version", "1.0");
        properties.setProperty("jet.build", "1486562404303");
        properties.setProperty("jet.git.revision", "a252185d2d39c8ed5ef2596e889307d396a239cc");

        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
        BuildInfoProvider.setJetProperties(properties, buildInfo);

        JetBuildInfo jetBuildInfo = buildInfo.getJetBuildInfo();

        assertEquals("1.0", jetBuildInfo.getVersion());
        assertEquals("1486562404303", jetBuildInfo.getBuild());
        assertEquals("a252185d2d39c8ed5ef2596e889307d396a239cc", jetBuildInfo.getRevision());

        assertContains(jetBuildInfo.toString(), "1.0");
        assertContains(jetBuildInfo.toString(), "1486562404303");
        assertContains(jetBuildInfo.toString(), "a252185d2d39c8ed5ef2596e889307d396a239cc");
    }
}
