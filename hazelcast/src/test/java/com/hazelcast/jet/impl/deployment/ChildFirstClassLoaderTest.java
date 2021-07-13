/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.test.JarUtil;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ChildFirstClassLoaderTest {

    private static URL jarUrl;
    private ChildFirstClassLoader cl;

    @BeforeClass
    public static void beforeClass() throws Exception {
        File jarFile = File.createTempFile("resources_", ".jar");
        JarUtil.createResourcesJarFile(jarFile);
        jarUrl = jarFile.toURI().toURL();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (jarUrl != null) {
            Files.delete(Paths.get(jarUrl.toURI()));
        }
    }

    @Test
    public void urlsMustNotBeNullNorEmpty() {
        assertThatThrownBy(() -> new ChildFirstClassLoader(null, ClassLoader.getSystemClassLoader()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ChildFirstClassLoader(new URL[0], ClassLoader.getSystemClassLoader()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void parentMustNotBeNull() {
        assertThatThrownBy(() -> new ChildFirstClassLoader(new URL[]{new URL("file://somefile.jar")}, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void canLoadClassFromParentClassLoader() throws Exception {
        cl = new ChildFirstClassLoader(new URL[]{new URL("file://somefile.jar")}, ChildFirstClassLoader.class.getClassLoader());

        Class<?> clazz = cl.loadClass(ChildFirstClassLoaderTest.class.getName());
        assertThat(clazz).isSameAs(this.getClass());
    }

    @Test
    public void canLoadClassFromChildClassLoaderWhenNotPresentInParent() throws Exception {
        cl = new ChildFirstClassLoader(new URL[]{resourceJarUrl("deployment/sample-pojo-1.0-car.jar")}, ClassLoader.getSystemClassLoader());

        String className = "com.sample.pojo.car.Car";
        Class<?> clazz = cl.loadClass(className);
        assertThat(clazz).isNotNull();
        assertThat(clazz.getName()).isEqualTo(className);
        assertThat(clazz.getClassLoader()).isEqualTo(cl);
    }

    @Test
    public void canLoadClassFromChildClassLoaderWhenPresentInParentClassloader() throws Exception {
        URL testClassesUrl = new File("./target/test-classes").toURI().toURL();
        cl = new ChildFirstClassLoader(new URL[]{testClassesUrl}, ChildFirstClassLoader.class.getClassLoader());

        String className = ChildFirstClassLoaderTest.class.getName();
        Class<?> clazz = cl.loadClass(className);
        assertThat(clazz).isNotNull();
        assertThat(clazz.getName()).isEqualTo(className);
        assertThat(clazz.getClassLoader()).isEqualTo(cl);
    }

    @Test
    public void canLoadResourceFromParentClassLoader() throws Exception {
        cl = new ChildFirstClassLoader(new URL[]{jarUrl},
                ChildFirstClassLoader.class.getClassLoader());

        String content = readResource("childfirstclassloader/resource_test_only.txt");
        assertThat(content).isEqualTo("resource in test resources");
    }

    @Test
    public void canLoadResourceFromChildClassLoaderWhenNotPresentInParent() throws Exception {
        cl = new ChildFirstClassLoader(new URL[]{jarUrl},
                ChildFirstClassLoader.class.getClassLoader());

        String content = readResource("childfirstclassloader/resource_jar.txt");
        assertThat(content).isEqualTo("resource in jar");
    }

    @Test
    public void canLoadResourceFromChildClassLoaderWhenPresentInParent() throws Exception {
        cl = new ChildFirstClassLoader(new URL[]{jarUrl},
                ChildFirstClassLoader.class.getClassLoader());

        String content = readResource("childfirstclassloader/resource_test.txt");
        assertThat(content).isEqualTo("resource in jar");
    }

    @Test
    public void shouldReturnResourcesFromAllClassloaders() throws Exception {
        cl = new ChildFirstClassLoader(new URL[]{jarUrl},
                ChildFirstClassLoader.class.getClassLoader());

        Enumeration<URL> urlsEnum = cl.getResources("childfirstclassloader/resource_test.txt");
        List<String> urls = new ArrayList<>();
        while (urlsEnum.hasMoreElements()) {
            urls.add(urlsEnum.nextElement().toString());
        }

        assertThat(urls).hasSize(2);
    }

    private URL resourceJarUrl(String name) {
        return Thread.currentThread().getContextClassLoader().getResource(name);
    }

    private String readResource(String name) throws IOException {
        InputStream is = cl.getResourceAsStream(name);
        if (is == null) {
            throw new IllegalArgumentException("Resource with name " + name +
                                               " could not be found in classloader " + cl);
        }
        return IOUtils.toString(is, UTF_8);
    }
}
