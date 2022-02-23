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

package com.hazelcast.jet.config;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ResourceConfigTest extends JetTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private JobConfig config;
    private File baseDir;

    @Before
    public void setup() throws IOException {
        config = new JobConfig();
        baseDir = createTempDirectory();
    }

    @After
    public void after() {
        com.hazelcast.internal.nio.IOUtil.delete(baseDir);
    }

    @Test
    public void when_addClassWithClass() {
        // When
        config.addClass(this.getClass());

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(toId(this.getClass()), resourceConfig.getId());
        assertEquals(ResourceType.CLASS, resourceConfig.getResourceType());
    }

    @Test
    public void when_addResourcesWithPackage() {
        // When
        config.addPackage(this.getClass().getPackage().getName());

        // Then
        Collection<ResourceConfig> resourceConfigs = config.getResourceConfigs().values();
        assertTrue(resourceConfigs
                .stream()
                .anyMatch(resourceConfig ->
                        resourceConfig.getId().equals(toId(this.getClass())) &&
                                resourceConfig.getResourceType().equals(ResourceType.CLASS)
                ));
        assertTrue(resourceConfigs
                .stream()
                .anyMatch(resourceConfig ->
                        resourceConfig.getId().contains("package.properties") &&
                                resourceConfig.getResourceType().equals(ResourceType.CLASSPATH_RESOURCE)
                ));
    }

    @Test
    public void when_addResourcesWithNonExistingPackage() {
        // When
        config.addPackage("thispackage.does.not.exist");

        // Then
        Collection<ResourceConfig> resourceConfigs = config.getResourceConfigs().values();
        assertTrue(resourceConfigs.isEmpty());
    }

    @Test
    public void when_addJarWithUrl() throws Exception {
        // Given
        String resourceId = "jarfile";
        URL url = new URL("http://site/path/to/" + resourceId);

        // When
        config.addJar(url);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(resourceId, resourceConfig.getId());
        assertEquals(ResourceType.JAR, resourceConfig.getResourceType());
        assertEquals(url, resourceConfig.getUrl());
    }

    @Test
    public void when_addJarWithUrlNoPath_then_throwsException() throws Exception {
        // Given
        URL url = new URL("http://hazelcast.org");

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.addJar(url);
    }

    @Test
    public void when_addDuplicateJarWithUrl_then_throwsException() throws Exception {
        // Given
        String resourceId = "file";
        URL url1 = new URL("http://site/" + resourceId);
        URL url2 = new URL("http://another.site/" + resourceId);
        config.addJar(url1);

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.addJar(url2);
    }

    private ResourceConfig getFirstResourceConfig() {
        return config.getResourceConfigs().values().iterator().next();
    }

    @Test
    public void when_addJarWithFile() throws Exception {
        // Given
        String resourceId = "jarfile";
        File file = createFile("path/to/" + resourceId);

        // When
        config.addJar(file);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(resourceId, resourceConfig.getId());
        assertEquals(ResourceType.JAR, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void when_addDuplicateJarWithFile_then_throwsException() throws Exception {
        // Given
        String resourceId = "file";
        File file1 = createFile("path/to/" + resourceId);
        File file2 = createFile("path/to/another/" + resourceId);
        config.addJar(file1);

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.addJar(file2);
    }

    @Test
    public void when_addNonexistentJarWithFile_then_throwsException() {
        // Given
        String path = Paths.get("/i/do/not/exist").toString();
        File file = new File(path);

        // Then
        expectedException.expect(JetException.class);
        expectedException.expectMessage("Not an existing, readable file: " + path);

        // When
        config.addJar(file);
    }

    @Test
    public void when_addJarWithPath() throws Exception {
        // Given
        String resourceId = "jarfile";
        File file = createFile("path/to/" + resourceId);
        String path = file.toString();

        // When
        config.addJar(path);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(resourceId, resourceConfig.getId());
        assertEquals(ResourceType.JAR, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void when_addDuplicateJarWithPath_then_throwsException() throws Exception {
        // Given
        String resourceId = "jarfile";
        String path1 = createFile("path/to/" + resourceId).toString();
        String path2 = createFile("path/to/another/" + resourceId).toString();
        config.addJar(path1);

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.addJar(path2);
    }

    @Test
    public void when_addNonexistentJarWithPath_then_throwsException() {
        // Given
        String path = Paths.get("/i/do/not/exist").toString();

        // Then
        expectedException.expect(JetException.class);
        expectedException.expectMessage("Not an existing, readable file: " + path);

        // When
        config.addJar(path);
    }

    @Test
    public void when_addZipOfJarWithUrl() throws Exception {
        // Given
        String resourceId = "zipFile";
        URL url = new URL("http://path/to/" + resourceId);

        // When
        config.addJarsInZip(url);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(resourceId, resourceConfig.getId());
        assertEquals(ResourceType.JARS_IN_ZIP, resourceConfig.getResourceType());
        assertEquals(url, resourceConfig.getUrl());
    }

    @Test
    public void when_addZipOfJarWithUrlNoPath_then_throwsException() throws Exception {
        // Given
        URL url = new URL("http://hazelcast.org");

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.addJarsInZip(url);
    }

    @Test
    public void when_addDuplicateZipOfJarWithUrl_then_throwsException() throws Exception {
        // Given
        String resourceId = "zipfile";
        URL url1 = new URL("http://site/" + resourceId);
        URL url2 = new URL("http://another.site/" + resourceId);
        config.addJarsInZip(url1);

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.addJarsInZip(url2);
    }

    @Test
    public void when_addZipOfJarWithPath() throws Exception {
        // Given
        String resourceId = "zipFile";
        File file = createFile("path/to/" + resourceId);
        String path = file.toString();

        // When
        config.addJarsInZip(path);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(resourceId, resourceConfig.getId());
        assertEquals(ResourceType.JARS_IN_ZIP, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void when_addDuplicateZipOfJarWithPath_then_throwsException() throws Exception {
        // Given
        String resourceId = "zipFile";
        String path1 = createFile("path/to/" + resourceId).toString();
        String path2 = createFile("path/to/another/" + resourceId).toString();
        config.addJarsInZip(path1);

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.addJarsInZip(path2);
    }

    @Test
    public void when_addNonexistentZipOfJarsWithPath_then_throwsException() {
        // Given
        String path = Paths.get("/i/do/not/exist").toString();

        // Then
        expectedException.expect(JetException.class);
        expectedException.expectMessage("Not an existing, readable file: " + path);

        // When
        config.addJarsInZip(path);
    }

    @Test
    public void when_addZipOfJarWithFile() throws Exception {
        // Given
        String resourceId = "zipFile";
        File file = createFile("path/to/" + resourceId);

        // When
        config.addJarsInZip(file);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(resourceId, resourceConfig.getId());
        assertEquals(ResourceType.JARS_IN_ZIP, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void when_addDuplicateZipOfJarWithFile_then_throwsException() throws Exception {
        // Given
        String resourceId = "zipFile";
        File file1 = createFile("path/to/" + resourceId);
        File file2 = createFile("path/to/another/" + resourceId);
        config.addJarsInZip(file1);

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.addJarsInZip(file2);
    }

    @Test
    public void when_addNonexistentZipOfJarsWithFile_then_throwsException() {
        // Given
        String path = Paths.get("/i/do/not/exist").toString();
        File file = new File(path);

        // Then
        expectedException.expect(JetException.class);
        expectedException.expectMessage("Not an existing, readable file: " + path);

        // When
        config.addJarsInZip(file);
    }

    @Test
    public void when_addResourceWithPath() throws Exception {
        // Given
        String resourceId = "my.txt";
        File file = createFile("path/to/" + resourceId);
        String path = file.toString();

        // When
        config.addClasspathResource(path);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(ResourceType.CLASSPATH_RESOURCE, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
        assertEquals(resourceId, resourceConfig.getId());
    }

    @Test
    public void when_addDuplicateResourceWithPath_then_throwsException() throws Exception {
        // Given
        String resourceId = "my.txt";
        String path1 = createFile("path/to/" + resourceId).toString();
        String path2 = createFile("path/to/another/" + resourceId).toString();
        config.addClasspathResource(path1);

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.addClasspathResource(path2);
    }

    @Test
    public void when_addNonexistentResourceWithPath_then_throwsException() {
        // Given
        String path = Paths.get("/i/do/not/exist").toString();

        // Then
        expectedException.expect(JetException.class);
        expectedException.expectMessage("Not an existing, readable file: " + path);

        // When
        config.addClasspathResource(path);
    }

    @Test
    public void when_addResourceWithPathAndId() throws Exception {
        // Given
        File file = createFile("path/to/my.txt");
        String path = file.toString();
        String resourceId = "customId";

        // When
        config.addClasspathResource(path, resourceId);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(ResourceType.CLASSPATH_RESOURCE, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
        assertEquals(resourceId, resourceConfig.getId());
    }

    @Test
    public void when_addNonexistentResourceWithPathAndId_then_throwsException() {
        // Given
        String id = "exist";
        String path = Paths.get("/i/do/not/" + id).toString();

        // Then
        expectedException.expect(JetException.class);
        expectedException.expectMessage("Not an existing, readable file: " + path);

        // When
        config.addClasspathResource(path, id);
    }

    @Test
    public void when_addResourceWithFile() throws Exception {
        // Given
        String resourceId = "my.txt";
        File file = createFile("path/to/" + resourceId);

        // When
        config.addClasspathResource(file);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(ResourceType.CLASSPATH_RESOURCE, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
        assertEquals(resourceId, resourceConfig.getId());
    }

    @Test
    public void when_addDuplicateResourceWithFile_then_throwsException() throws Exception {
        // Given
        String resourceId = "my.txt";
        File file1 = createFile("path/to/" + resourceId);
        File file2 = createFile("path/to/another/" + resourceId);
        config.addClasspathResource(file1);

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.addClasspathResource(file2);
    }

    @Test
    public void when_addNonexistentResourceWithFile_then_throwsException() {
        // Given
        String path = Paths.get("/i/do/not/exist").toString();
        File file = new File(path);

        // Then
        expectedException.expect(JetException.class);
        expectedException.expectMessage("Not an existing, readable file: " + path);

        // When
        config.addClasspathResource(file);
    }

    @Test
    public void when_addResourceWithFileAndId() throws Exception {
        // Given
        File file = createFile("path/to/my.txt");
        String id = "customId";

        // When
        config.addClasspathResource(file, id);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(ResourceType.CLASSPATH_RESOURCE, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
        assertEquals(id, resourceConfig.getId());
    }

    @Test
    public void when_addNonexistentResourceWithFileAndId_then_throwsException() {
        // Given
        String id = "exist";
        String path = Paths.get("/i/do/not/" + id).toString();
        File file = new File(path);

        // Then
        expectedException.expect(JetException.class);
        expectedException.expectMessage("Not an existing, readable file: " + path);

        // When
        config.addClasspathResource(file, id);
    }

    @Test
    public void when_addResourceWithURL() throws Exception {
        // Given
        String resourceId = "my.txt";
        URL url = new URL("http://site/" + resourceId);

        // When
        config.addClasspathResource(url);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(ResourceType.CLASSPATH_RESOURCE, resourceConfig.getResourceType());
        assertEquals(url, resourceConfig.getUrl());
        assertEquals(resourceId, resourceConfig.getId());
    }

    @Test
    public void when_addDuplicateResourceWithUrl_then_throwsException() throws Exception {
        // Given
        String resourceId = "my.txt";
        URL url1 = new URL("http://site/" + resourceId);
        URL url2 = new URL("http://another.site/" + resourceId);
        config.addClasspathResource(url1);

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.addClasspathResource(url2);
    }

    @Test
    public void when_addResourceWithUrlNoPath_then_throwsException() throws Exception {
        // Given
        URL url = new URL("http://hazelcast.org");

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.addClasspathResource(url);
    }

    @Test
    public void when_addDuplicateResourceWithUrlAndId_then_throwsException() throws Exception {
        // Given
        String id = "resourceFileName";
        File file = createFile("path/to/resource");
        config.addClasspathResource(file, id);

        // Then
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(id);

        // When
        config.addClasspathResource(file, id);
    }

    @Test
    public void when_addResourceWithURLAndId() throws Exception {
        // Given
        String resourceId = "resourceId";
        URL url = new URL("http://site/my.txt");

        // When
        config.addClasspathResource(url, resourceId);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(ResourceType.CLASSPATH_RESOURCE, resourceConfig.getResourceType());
        assertEquals(url, resourceConfig.getUrl());
        assertEquals(resourceId, resourceConfig.getId());
    }

    @Test
    public void when_attachFileWithUrl() throws Exception {
        // Given
        String resourceId = "resourceId";
        URL url = new URL("http://site/" + resourceId);

        // When
        config.attachFile(url);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(resourceId, resourceConfig.getId());
        assertEquals(ResourceType.FILE, resourceConfig.getResourceType());
        assertEquals(url, resourceConfig.getUrl());
    }

    @Test
    public void when_attachDuplicateFileWithUrl_then_throwsException() throws Exception {
        // Given
        String resourceId = "resourceId";
        URL url1 = new URL("http://site/" + resourceId);
        URL url2 = new URL("http://another.site/" + resourceId);
        config.attachFile(url1);

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.attachFile(url2);
    }

    @Test
    public void when_attachFileWithUrlNoPath_then_throwsException() throws Exception {
        // Given
        URL url = new URL("http://hazelcast.org");

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.attachFile(url);
    }

    @Test
    public void when_attachFileWithUrlAndId() throws Exception {
        // Given
        String id = "resourceId";
        URL url = createFile("path/to/resourceFile").toURI().toURL();

        // When
        config.attachFile(url, id);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(id, resourceConfig.getId());
        assertEquals(ResourceType.FILE, resourceConfig.getResourceType());
        assertEquals(url, resourceConfig.getUrl());
    }

    @Test
    public void when_attachFileWithPath() throws Exception {
        // Given
        String resourceId = "resource";
        File file = createFile("path/to/" + resourceId);
        String path = file.toString();

        // When
        config.attachFile(path);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(resourceId, resourceConfig.getId());
        assertEquals(ResourceType.FILE, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void when_attachDuplicateFileWithPath_then_throwsException() throws Exception {
        // Given
        String resourceId = "resource";
        String path1 = createFile("path/to/" + resourceId).toString();
        String path2 = createFile("path/to/another/" + resourceId).toString();
        config.attachFile(path1);

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.attachFile(path2);
    }

    @Test
    public void when_attachNonexistentFileWithPath_then_throwsException() {
        // Given
        String path = Paths.get("/i/do/not/exist").toString();

        // Then
        expectedException.expect(JetException.class);
        expectedException.expectMessage("Not an existing, readable file: " + path);

        // When
        config.attachFile(path);
    }

    @Test
    public void when_attachFileWithPathAndId() throws Exception {
        // Given
        String id = "resourceId";
        File file = createFile("path/to/jarfile");
        String path = file.toString();

        // When
        config.attachFile(path, id);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(id, resourceConfig.getId());
        assertEquals(ResourceType.FILE, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void when_attachNonexistentFileWithPathAndId_then_throwsException() {
        // Given
        String id = "exist";
        String path = Paths.get("/i/do/not/" + id).toString();

        // Then
        expectedException.expect(JetException.class);
        expectedException.expectMessage("Not an existing, readable file: " + path);

        // When
        config.attachFile(path, id);
    }

    @Test
    public void when_attachFileWithFile() throws Exception {
        // Given
        String resourceId = "resource";
        File file = createFile("path/to/" + resourceId);

        // When
        config.attachFile(file);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(resourceId, resourceConfig.getId());
        assertEquals(ResourceType.FILE, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void when_attachDuplicateFileWithFile_then_throwsException() throws Exception {
        // Given
        String resourceId = "resource";
        File file1 = createFile("path/to/" + resourceId);
        File file2 = createFile("path/to/another/" + resourceId);
        config.attachFile(file1);

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.attachFile(file2);
    }

    @Test
    public void when_attachNonexistentFileWithFile_then_throwsException() {
        // Given
        String path = Paths.get("/i/do/not/exist").toString();
        File file = new File(path);

        // Then
        expectedException.expect(JetException.class);
        expectedException.expectMessage("Not an existing, readable file: " + path);

        // When
        config.attachFile(file);
    }

    @Test
    public void when_attachFileWithFileAndId() throws Exception {
        // Given
        String id = "resourceId";
        File file = createFile("path/to/resource");

        // When
        config.attachFile(file, id);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(id, resourceConfig.getId());
        assertEquals(ResourceType.FILE, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void when_attachNonexistentFileWithFileAndId_then_throwsException() {
        // Given
        String id = "exist";
        String path = Paths.get("/i/do/not/" + id).toString();
        File file = new File(path);

        // Then
        expectedException.expect(JetException.class);
        expectedException.expectMessage("Not an existing, readable file: " + path);

        // When
        config.attachFile(file, id);
    }

    @Test
    public void when_attachFileWithDuplicateId_then_throwsException() throws Exception {
        // Given
        String id = "resourceId";
        File file = createFile("path/to/resource");
        config.attachFile(file, id);

        // Then
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(id);

        // When
        config.attachFile(file, id);
    }

    @Test
    public void when_attachDirectoryWithUrl() throws Exception {
        // Given
        String resourceId = "resourceDirectory";
        String path = "path/to/" + resourceId;
        URL url = createDirectory(path).toURI().toURL();

        // When
        config.attachDirectory(url);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(resourceId, resourceConfig.getId());
        assertEquals(ResourceType.DIRECTORY, resourceConfig.getResourceType());
        assertEquals(url, resourceConfig.getUrl());
    }

    @Test
    public void when_attachDuplicateDirectoryWithUrl_then_throwsException() throws Exception {
        // Given
        URL url1 = createDirectory("path/to/dir").toURI().toURL();
        URL url2 = createDirectory("path/to/another/dir").toURI().toURL();
        config.attachDirectory(url1);

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.attachDirectory(url2);
    }

    @Test
    public void when_attachNonexistentDirectoryWithUrl_then_throwsException() throws Exception {
        // Given
        String path = "/i/do/not/exist";
        URL url = new File(path).toURI().toURL();

        // Then
        expectedException.expect(JetException.class);
        expectedException.expectMessage("Not an existing, readable directory: ");

        // When
        config.attachDirectory(url);
    }

    @Test
    public void when_attachDirectoryWithUrlAndId() throws Exception {
        // Given
        String resourceId = "resourceId";
        URL url = createDirectory("path/to/resourceDirectory").toURI().toURL();

        // When
        config.attachDirectory(url, resourceId);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(resourceId, resourceConfig.getId());
        assertEquals(ResourceType.DIRECTORY, resourceConfig.getResourceType());
        assertEquals(url, resourceConfig.getUrl());
    }

    @Test
    public void when_attachDirectoryWithPath() throws Exception {
        // Given
        String resourceId = "directory";
        File directory = createDirectory("path/to/" + resourceId);
        String path = directory.toString();

        // When
        config.attachDirectory(path);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(resourceId, resourceConfig.getId());
        assertEquals(ResourceType.DIRECTORY, resourceConfig.getResourceType());
        assertEquals(directory.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void when_attachDuplicateDirectoryWithPath_then_throwsException() {
        // Given
        String path1 = createDirectory("path/to/dir").toString();
        String path2 = createDirectory("path/to/another/dir").toString();
        config.attachDirectory(path1);

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.attachDirectory(path2);
    }

    @Test
    public void when_attachNonexistentDirectoryWithPath_then_throwsException() {
        // Given
        String path = "/i/do/not/exist";

        // Then
        expectedException.expect(JetException.class);
        expectedException.expectMessage("Not an existing, readable directory: ");

        // When
        config.attachDirectory(path);
    }

    @Test
    public void when_attachDirectoryWithPathAndId() throws Exception {
        // Given
        String resourceId = "resourceId";
        File file = createDirectory("path/to/jarfile");
        String path = file.toString();

        // When
        config.attachDirectory(path, resourceId);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(resourceId, resourceConfig.getId());
        assertEquals(ResourceType.DIRECTORY, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void when_attachNonexistentDirectoryWithPathAndId_then_throwsException() {
        // Given
        String id = "exist";
        String path = "/i/do/not/" + id;

        // Then
        expectedException.expect(JetException.class);
        expectedException.expectMessage("Not an existing, readable directory: ");

        // When
        config.attachDirectory(path, id);
    }

    @Test
    public void when_attachDirectoryWithFile() throws Exception {
        // Given
        String resourceId = "resource";
        File file = createDirectory("path/to/" + resourceId);

        // When
        config.attachDirectory(file);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(resourceId, resourceConfig.getId());
        assertEquals(ResourceType.DIRECTORY, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void when_attachDuplicateDirectoryWithFile_then_throwsException() {
        // Given
        String resourceId = "dir";
        File dir1 = createDirectory("path/to/" + resourceId);
        File dir2 = createDirectory("path/to/another/" + resourceId);
        config.attachDirectory(dir1);

        // Then
        expectedException.expect(IllegalArgumentException.class);

        // When
        config.attachDirectory(dir2);
    }

    @Test
    public void when_attachNonexistentDirectoryWithFile_then_throwsException() {
        // Given
        String path = "/i/do/not/exist";
        File file = new File(path);

        // Then
        expectedException.expect(JetException.class);
        expectedException.expectMessage("Not an existing, readable directory: ");

        // When
        config.attachDirectory(file);
    }

    @Test
    public void when_attachDirectoryWithFileAndId() throws Exception {
        // Given
        String resourceId = "resourceId";
        File file = createDirectory("path/to/resource");

        // When
        config.attachDirectory(file, resourceId);

        // Then
        ResourceConfig resourceConfig = getFirstResourceConfig();
        assertEquals(resourceId, resourceConfig.getId());
        assertEquals(ResourceType.DIRECTORY, resourceConfig.getResourceType());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void when_attachNonexistentDirectoryWithFileAndId_then_throwsException() {
        // Given
        String id = "exist";
        String path = "/i/do/not/" + id;
        File file = new File(path);

        // Then
        expectedException.expect(JetException.class);
        expectedException.expectMessage("Not an existing, readable directory: ");

        // When
        config.attachDirectory(file, id);
    }

    @Test
    public void when_attachDuplicateDirectoryWithFileAndId_then_throwsException() {
        // Given
        String id = "dirName";
        File file = createDirectory("path/to/dirName");
        config.attachDirectory(file, id);

        // Then
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(id);

        // When
        config.attachDirectory(file, id);
    }

    @Test
    public void when_attachAll() throws Exception {
        // Given
        String fileId = "fileId";
        String dirId = "dirId";
        File file = createFile("path/to/file");
        File dir = createDirectory("path/to/directory");
        Map<String, File> attachments = new HashMap<>();
        attachments.put(fileId, file);
        attachments.put(dirId, dir);

        // When
        config.attachAll(attachments);

        // Then
        Map<String, ResourceConfig> resourceConfigs = config.getResourceConfigs();

        ResourceConfig fileConfig = resourceConfigs.get(fileId);
        assertEquals(fileId, fileConfig.getId());
        assertEquals(ResourceType.FILE, fileConfig.getResourceType());
        assertEquals(file.toURI().toURL(), fileConfig.getUrl());

        ResourceConfig dirConfig = resourceConfigs.get(dirId);
        assertEquals(dirId, dirConfig.getId());
        assertEquals(ResourceType.DIRECTORY, dirConfig.getResourceType());
        assertEquals(dir.toURI().toURL(), dirConfig.getUrl());
    }

    private File createFile(String path) throws IOException {
        File file = new File(baseDir, path);
        assertTrue("Failed to create parent path for " + file, file.getParentFile().mkdirs());
        assertTrue("Failed to create file " + file, file.createNewFile());
        return file;
    }

    private File createDirectory(String path) {
        File dirFile = new File(baseDir, path);
        assertTrue("Failed to create directory " + dirFile, dirFile.mkdirs());
        return dirFile;
    }

    private static String toId(Class<?> clazz) {
        return clazz.getName().replace('.', '/') + ".class";
    }
}
