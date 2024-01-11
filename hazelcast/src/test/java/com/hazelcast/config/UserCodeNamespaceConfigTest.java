/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.config.ResourceType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import usercodedeployment.Person;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static com.hazelcast.jet.impl.util.ReflectionUtils.toClassResourceId;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UserCodeNamespaceConfigTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private UserCodeNamespaceConfig userCodeNamespaceConfig = new UserCodeNamespaceConfig();

    @Test
    public void testName() {
        String name = randomString();
        userCodeNamespaceConfig.setName(name);
        assertEquals(name, userCodeNamespaceConfig.getName());
    }

    @Test (expected = NullPointerException.class)
    public void testNullName() {
        userCodeNamespaceConfig.setName(null);
    }

    @Test
    public void testAddClass() {
        Class<?> clazz = Person.class;
        userCodeNamespaceConfig.addClass(clazz);
        ClassLoader cl = clazz.getClassLoader();
        assertNotNull(clazz.getName() + ".getClassLoader() returned null", cl);
        String classResourceId = toClassResourceId(clazz.getName());

        long matches = userCodeNamespaceConfig.getResourceConfigs().stream()
                                              .filter(rc -> rc.type().equals(ResourceType.CLASS))
                                              .filter(rc -> rc.id().equals(classResourceId))
                                              .filter(rc -> rc.url().equals(cl.getResource(classResourceId).toString()))
                                              .count();
        assertEquals(1, matches);
    }

    @Test
    public void testAddJar_WithNullID() throws IOException {
        Path fakeResource = createFakeResource("location/of/my/jars/ExampleJar.jar");
        URL fakeUrl = fakeResource.toUri().toURL();
        userCodeNamespaceConfig.addJar(fakeUrl, null);

        long matches = userCodeNamespaceConfig.getResourceConfigs().stream()
                                              .filter(rc -> rc.type().equals(ResourceType.JAR))
                                              .filter(rc -> rc.id().equals("ExampleJar.jar"))
                                              .filter(rc -> rc.url().equals(fakeUrl.toString()))
                                              .count();
        assertEquals(1, matches);
    }

    @Test
    public void testAddJar_WithCustomID() throws IOException {
        Path fakeResource = createFakeResource("location/of/my/jars/ExampleJar.jar");
        URL fakeUrl = fakeResource.toUri().toURL();
        userCodeNamespaceConfig.addJar(fakeUrl, "MyCustomID");

        long matches = userCodeNamespaceConfig.getResourceConfigs().stream()
                                              .filter(rc -> rc.type().equals(ResourceType.JAR))
                                              .filter(rc -> rc.id().equals("MyCustomID"))
                                              .filter(rc -> rc.url().equals(fakeUrl.toString()))
                                              .count();
        assertEquals(1, matches);
    }

    @Test
    public void testAddJarsInZip_WithNullID() throws IOException {
        Path fakeResource = createFakeResource("location/of/my/zips/ExampleZip.zip");
        URL fakeUrl = fakeResource.toUri().toURL();
        userCodeNamespaceConfig.addJarsInZip(fakeUrl, null);

        long matches = userCodeNamespaceConfig.getResourceConfigs().stream()
                                              .filter(rc -> rc.type().equals(ResourceType.JARS_IN_ZIP))
                                              .filter(rc -> rc.id().equals("ExampleZip.zip"))
                                              .filter(rc -> rc.url().equals(fakeUrl.toString()))
                                              .count();
        assertEquals(1, matches);
    }

    @Test
    public void testAddJarsInZip_WithCustomID() throws IOException {
        Path fakeResource = createFakeResource("location/of/my/zips/ExampleZip.zip");
        URL fakeUrl = fakeResource.toUri().toURL();
        userCodeNamespaceConfig.addJarsInZip(fakeUrl, "MyZipID");

        long matches = userCodeNamespaceConfig.getResourceConfigs().stream()
                                              .filter(rc -> rc.type().equals(ResourceType.JARS_IN_ZIP))
                                              .filter(rc -> rc.id().equals("MyZipID"))
                                              .filter(rc -> rc.url().equals(fakeUrl.toString()))
                                              .count();
        assertEquals(1, matches);
    }

    private Path createFakeResource(String resourceUrl) throws IOException {
        assert resourceUrl.length() > 1;
        String[] parts = resourceUrl.split("/");
        String[] directory_parts = new String[parts.length - 1];
        System.arraycopy(parts, 0, directory_parts, 0, directory_parts.length);
        File folder = temporaryFolder.newFolder(directory_parts);
        return Files.write(folder.toPath().resolve(parts[parts.length - 1]), Collections.singletonList("Fluff-data"));
    }
}
