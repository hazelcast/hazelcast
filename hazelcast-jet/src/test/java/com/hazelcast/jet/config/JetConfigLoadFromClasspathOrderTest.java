/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.config;

import com.hazelcast.jet.test.FilteringAndDelegatingResourceLoadingClassLoader;
import com.hazelcast.jet.test.SerialTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SerialTest.class})
public class JetConfigLoadFromClasspathOrderTest {

    @Test
    public void when_YamlMemberConfigPresentInClasspathAlongWithDefaultConfig_usesMemberConfig() throws Exception {
        // Given
        URL url = Thread.currentThread().getContextClassLoader().getResource("config-test-yaml-first/");
        URLClassLoader delegate = new URLClassLoader(new URL[] {url});
        testWithClassloaderDelegate(delegate);
    }

    @Test
    public void when_XmlMemberConfigPresentInClasspathAlongWithDefaultConfig_usesMemberConfig() throws Exception {
        // Given
        URL url = Thread.currentThread().getContextClassLoader().getResource("config-test-xml-first/");
        URLClassLoader delegate = new URLClassLoader(new URL[] {url});
        testWithClassloaderDelegate(delegate);
    }

    private void testWithClassloaderDelegate(URLClassLoader delegate) throws Exception {
        FilteringAndDelegatingResourceLoadingClassLoader contextClassLoader =
                new FilteringAndDelegatingResourceLoadingClassLoader(
                        emptyList(),
                        "com.hazelcast",
                        delegate
                );
        Thread.currentThread().setContextClassLoader(contextClassLoader);
        ClassLoader cl = Thread.currentThread().getContextClassLoader();

        // When
        Class<?> configClazz = cl.loadClass("com.hazelcast.config.Config");
        Class<?> jetConfigClazz = cl.loadClass("com.hazelcast.jet.config.JetConfig");
        Object jetConfig = jetConfigClazz.newInstance();

        Method loadDefault = jetConfigClazz.getDeclaredMethod("loadDefault");
        Object loadedConfig = loadDefault.invoke(jetConfig);

        Method getHazelcastConfig = jetConfigClazz.getDeclaredMethod("getHazelcastConfig");
        Object hazelcastConfig = getHazelcastConfig.invoke(loadedConfig);

        Method getProperty = configClazz.getDeclaredMethod("getProperty", String.class);
        Object propertyValue = getProperty.invoke(hazelcastConfig, "test-property");

        // Then
        assertThat(loadedConfig, not(nullValue()));
        assertThat(propertyValue, equalTo("test-value"));
    }

}
