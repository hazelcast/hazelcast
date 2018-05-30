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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
public class ResourceConfigTest {

    @Test
    public void testAddClass_with_Class() {
        JobConfig config = new JobConfig();
        config.addClass(this.getClass());
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals(this.getClass().getName().replace('.', '/') + ".class", resourceConfig.getId());
        assertFalse(resourceConfig.isArchive());
    }

    @Test
    public void testAddJar_with_Url() throws Exception {
        JobConfig config = new JobConfig();
        String urlString = "file://path/to/jarfile";
        config.addJar(new URL(urlString));
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertNull(resourceConfig.getId());
        assertTrue(resourceConfig.isArchive());
        assertEquals(urlString, resourceConfig.getUrl().toString());
    }

    @Test
    public void testAddJar_with_Path() throws Exception {
        JobConfig config = new JobConfig();
        String path = "/path/to/jarfile";
        config.addJar(path);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertNull(resourceConfig.getId());
        assertTrue(resourceConfig.isArchive());
        assertEquals(new File(path).toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void testAddJar_with_File() throws Exception {
        JobConfig config = new JobConfig();
        File file = new File("/path/to/jarfile");
        config.addJar(file);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertNull(resourceConfig.getId());
        assertTrue(resourceConfig.isArchive());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void testAddResource_with_Url() throws Exception {
        JobConfig config = new JobConfig();
        String urlString = "file://path/to/resourceFile";
        config.addResource(new URL(urlString));
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("resourceFile", resourceConfig.getId());
        assertFalse(resourceConfig.isArchive());
        assertEquals(urlString, resourceConfig.getUrl().toString());
    }

    @Test
    public void testAddResource_with_Url_and_ResourceName() throws Exception {
        JobConfig config = new JobConfig();
        String resourceName = "resourceFileName";
        String urlString = "file://path/to/resourceFile";
        config.addResource(new URL(urlString), resourceName);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals(resourceName, resourceConfig.getId());
        assertFalse(resourceConfig.isArchive());
        assertEquals(urlString, resourceConfig.getUrl().toString());
    }

    @Test
    public void testAddResource_with_Path() throws Exception {
        JobConfig config = new JobConfig();
        String path = "/path/to/resource";
        config.addResource(path);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("resource", resourceConfig.getId());
        assertFalse(resourceConfig.isArchive());
        assertEquals(new File(path).toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void testAddResource_with_Path_and_ResourceName() throws Exception {
        JobConfig config = new JobConfig();
        String resourceName = "resourceFileName";
        String path = "/path/to/jarfile";
        config.addResource(path, resourceName);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals(resourceName, resourceConfig.getId());
        assertFalse(resourceConfig.isArchive());
        assertEquals(new File(path).toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void testAddResource_with_File() throws Exception {
        JobConfig config = new JobConfig();
        File file = new File("/path/to/resource");
        config.addResource(file);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("resource", resourceConfig.getId());
        assertFalse(resourceConfig.isArchive());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }

    @Test
    public void testAddResource_with_File_and_ResourceName() throws Exception {
        JobConfig config = new JobConfig();
        String resourceName = "resourceFileName";
        File file = new File("/path/to/resource");
        config.addResource(file, resourceName);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals(resourceName, resourceConfig.getId());
        assertFalse(resourceConfig.isArchive());
        assertEquals(file.toURI().toURL(), resourceConfig.getUrl());
    }


}
