/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static org.junit.Assert.assertEquals;

/**
 * Test cases specific only to YAML based configuration. The cases not
 * XML specific should be added to {@link XMLConfigBuilderTest}.
 * <p/>
 * This test class is expected to contain only <strong>extra</strong> test
 * cases over the ones defined in {@link XMLConfigBuilderTest} in order
 * to cover XML specific cases where XML configuration derives from the
 * YAML configuration to allow usage of XML-native constructs.
 *
 * @see YamlConfigBuilderTest
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class XmlOnlyConfigBuilderTest {

    static final String HAZELCAST_START_TAG = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n";
    static final String HAZELCAST_END_TAG = "</hazelcast>\n";

    @Test(expected = InvalidConfigurationException.class)
    public void testMissingNamespace() {
        String xml = "<hazelcast/>";
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidNamespace() {
        String xml = "<hazelcast xmlns=\"http://foo.bar\"/>";
        buildConfig(xml);
    }

    @Test
    public void testValidNamespace() {
        String xml = HAZELCAST_START_TAG + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastTagAppearsTwice() {
        String xml = HAZELCAST_START_TAG + "<hazelcast/>" + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastInstanceNameEmpty() {
        String xml = HAZELCAST_START_TAG + "<instance-name></instance-name>" + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Test
    public void testXsdVersion() {
        String origVersionOverride = System.getProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
        assertXsdVersion("0.0", "0.0");
        assertXsdVersion("3.9", "3.9");
        assertXsdVersion("3.9-SNAPSHOT", "3.9");
        assertXsdVersion("3.9.1-SNAPSHOT", "3.9");
        assertXsdVersion("3.10", "3.10");
        assertXsdVersion("3.10-SNAPSHOT", "3.10");
        assertXsdVersion("3.10.1-SNAPSHOT", "3.10");
        assertXsdVersion("99.99.99", "99.99");
        assertXsdVersion("99.99.99-SNAPSHOT", "99.99");
        assertXsdVersion("99.99.99-Beta", "99.99");
        assertXsdVersion("99.99.99-Beta-SNAPSHOT", "99.99");
        if (origVersionOverride != null) {
            System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, origVersionOverride);
        } else {
            System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
        }
    }

    private void assertXsdVersion(String buildVersion, String expectedXsdVersion) {
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, buildVersion);
        assertEquals("Unexpected release version retrieved for build version " + buildVersion, expectedXsdVersion,
                new XmlConfigBuilder().getReleaseVersion());
    }

    private Config buildConfig(String xml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        return configBuilder.build();
    }
}
