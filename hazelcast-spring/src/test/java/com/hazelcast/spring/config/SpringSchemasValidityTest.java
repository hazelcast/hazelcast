/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring.config;

import com.hazelcast.test.annotation.QuickTest;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.springframework.util.ResourceUtils;

import java.io.FileInputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A unit test which validates the spring.schemas file
 */
@RunWith(JUnit4.class)
@Category(QuickTest.class)
public class SpringSchemasValidityTest {

    private Properties prop;

    @Before
    public void loadSpringSchemas() throws Exception {
        prop = new Properties();
        prop.load(new FileInputStream(ResourceUtils.getFile("classpath:META-INF/spring.schemas")));
    }

    @Test
    public void testAllXSDsAvailable() throws Exception {
        Collection<Object> values = prop.values();
        Set<String> allXSDs = new HashSet<>();

        for (Object o : values) {
            allXSDs.add(o.toString());
        }

        for (String xsd : allXSDs) {
            assertTrue(ResourceUtils.getFile("classpath:" + xsd).exists());
        }
    }

    @Test
    public void testUrlMatchesXSD() {
        Set<String> names = prop.stringPropertyNames();

        for (String name : names) {
            if (name.endsWith("spring.xsd")) {
                // perma link without version in file name
                continue;
            }
            assertTrue(name.endsWith(prop.getProperty(name)));
        }
    }

    @Test
    public void testPermaLinkIsLatestVersion() {
        Pattern pattern = Pattern.compile("hazelcast-spring-([0-9\\.]+)\\.xsd");
        ComparableVersion latestVersion = null;
        String latestVersionXsdFile = null;

        for (Object o: prop.values()) {
            String xsd = o.toString();

            Matcher matcher = pattern.matcher(xsd);
            assertTrue(matcher.matches());

            String versionCode = matcher.group(1);

            if (latestVersion == null) {
                latestVersion = new ComparableVersion(versionCode);
                latestVersionXsdFile = xsd;
            } else {
                ComparableVersion current = new ComparableVersion(versionCode);
                if (current.compareTo(latestVersion) > 0) {
                    latestVersion = current;
                    latestVersionXsdFile = xsd;
                }
            }
        }

        String latestBySpringSchemas = prop.getProperty("https://www.hazelcast.com/schema/spring/hazelcast-spring.xsd");
        assertEquals(latestVersionXsdFile, latestBySpringSchemas);
    }

    @Test
    public void testBothHttpAndHttpsAvailable() {
        Set<String> allURLs = prop.stringPropertyNames();

        Set<String> http = new HashSet<>();
        Set<String> https = new HashSet<>();

        for (String url : allURLs) {
            if (url.startsWith("https")) {
                https.add(url);
            } else if (url.startsWith("http")) {
                http.add(url);
            } else {
                fail(url + " does not start with http or https");
            }
        }

        assertEquals(http.size(), https.size());

        for (String s : https) {
            assertTrue(http.contains("http" + s.substring(5)));
        }

        for (String s : http) {
            assertTrue(https.contains("https" + s.substring(4)));
        }
    }
}
