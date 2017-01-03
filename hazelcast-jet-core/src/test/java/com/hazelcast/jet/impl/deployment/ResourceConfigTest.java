package com.hazelcast.jet.impl.deployment;

import com.hazelcast.jet.JobConfig;
import com.hazelcast.jet.ResourceConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import java.io.File;
import java.net.URL;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.impl.deployment.ResourceType.CLASS;
import static com.hazelcast.jet.impl.deployment.ResourceType.DATA;
import static com.hazelcast.jet.impl.deployment.ResourceType.JAR;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ResourceConfigTest {

    @Test
    public void testAddClass_with_Class() throws Exception {
        JobConfig config = new JobConfig();
        config.addClass(this.getClass());
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals(this.getClass().getName(), resourceConfig.getDescriptor().getId());
        assertEquals(CLASS, resourceConfig.getDescriptor().getResourceType());
    }

    @Test
    public void testAddJar_with_Url() throws Exception {
        JobConfig config = new JobConfig();
        String urlString = "file://path/to/jarfile";
        config.addJar(new URL(urlString));
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("jarfile", resourceConfig.getDescriptor().getId());
        assertEquals(JAR, resourceConfig.getDescriptor().getResourceType());
        assertEquals(urlString, resourceConfig.getUrl().toString());
    }

    @Test
    public void testAddJar_with_Url_and_JarName() throws Exception {
        JobConfig config = new JobConfig();
        String jarName = "jarFileName";
        String urlString = "file://path/to/jarfile";
        config.addJar(new URL(urlString), jarName);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals(jarName, resourceConfig.getDescriptor().getId());
        assertEquals(JAR, resourceConfig.getDescriptor().getResourceType());
        assertEquals(urlString, resourceConfig.getUrl().toString());
    }

    @Test
    public void testAddJar_with_Path() throws Exception {
        JobConfig config = new JobConfig();
        String path = "/path/to/jarfile";
        config.addJar(path);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("jarfile", resourceConfig.getDescriptor().getId());
        assertEquals(JAR, resourceConfig.getDescriptor().getResourceType());
        assertEquals(path, resourceConfig.getUrl().getPath());
    }

    @Test
    public void testAddJar_with_Path_and_JarName() throws Exception {
        JobConfig config = new JobConfig();
        String jarName = "jarFileName";
        String path = "/path/to/jarfile";
        config.addJar(path, jarName);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals(jarName, resourceConfig.getDescriptor().getId());
        assertEquals(JAR, resourceConfig.getDescriptor().getResourceType());
        assertEquals(path, resourceConfig.getUrl().getPath());
    }

    @Test
    public void testAddJar_with_File() throws Exception {
        JobConfig config = new JobConfig();
        File file = new File("/path/to/jarfile");
        config.addJar(file);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("jarfile", resourceConfig.getDescriptor().getId());
        assertEquals(JAR, resourceConfig.getDescriptor().getResourceType());
        assertEquals(file.getPath(), resourceConfig.getUrl().getPath());
    }

    @Test
    public void testAddJar_with_File_and_JarName() throws Exception {
        JobConfig config = new JobConfig();
        String jarName = "jarFileName";
        File file = new File("/path/to/jarfile");
        config.addJar(file, jarName);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals(jarName, resourceConfig.getDescriptor().getId());
        assertEquals(JAR, resourceConfig.getDescriptor().getResourceType());
        assertEquals(file.getPath(), resourceConfig.getUrl().getPath());
    }

    @Test
    public void testAddResource_with_Url() throws Exception {
        JobConfig config = new JobConfig();
        String urlString = "file://path/to/resourceFile";
        config.addResource(new URL(urlString));
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("resourceFile", resourceConfig.getDescriptor().getId());
        assertEquals(DATA, resourceConfig.getDescriptor().getResourceType());
        assertEquals(urlString, resourceConfig.getUrl().toString());
    }

    @Test
    public void testAddResource_with_Url_and_ResourceName() throws Exception {
        JobConfig config = new JobConfig();
        String resourceName = "resourceFileName";
        String urlString = "file://path/to/resourceFile";
        config.addResource(new URL(urlString), resourceName);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals(resourceName, resourceConfig.getDescriptor().getId());
        assertEquals(DATA, resourceConfig.getDescriptor().getResourceType());
        assertEquals(urlString, resourceConfig.getUrl().toString());
    }

    @Test
    public void testAddResource_with_Path() throws Exception {
        JobConfig config = new JobConfig();
        String path = "/path/to/resource";
        config.addResource(path);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("resource", resourceConfig.getDescriptor().getId());
        assertEquals(DATA, resourceConfig.getDescriptor().getResourceType());
        assertEquals(path, resourceConfig.getUrl().getPath());
    }

    @Test
    public void testAddResource_with_Path_and_ResourceName() throws Exception {
        JobConfig config = new JobConfig();
        String resourceName = "resourceFileName";
        String path = "/path/to/jarfile";
        config.addResource(path, resourceName);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals(resourceName, resourceConfig.getDescriptor().getId());
        assertEquals(DATA, resourceConfig.getDescriptor().getResourceType());
        assertEquals(path, resourceConfig.getUrl().getPath());
    }

    @Test
    public void testAddResource_with_File() throws Exception {
        JobConfig config = new JobConfig();
        File file = new File("/path/to/resource");
        config.addResource(file);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals("resource", resourceConfig.getDescriptor().getId());
        assertEquals(DATA, resourceConfig.getDescriptor().getResourceType());
        assertEquals(file.getPath(), resourceConfig.getUrl().getPath());
    }

    @Test
    public void testAddResource_with_File_and_ResourceName() throws Exception {
        JobConfig config = new JobConfig();
        String resourceName = "resourceFileName";
        File file = new File("/path/to/resource");
        config.addResource(file, resourceName);
        ResourceConfig resourceConfig = config.getResourceConfigs().iterator().next();

        assertEquals(resourceName, resourceConfig.getDescriptor().getId());
        assertEquals(DATA, resourceConfig.getDescriptor().getResourceType());
        assertEquals(file.getPath(), resourceConfig.getUrl().getPath());
    }


}
