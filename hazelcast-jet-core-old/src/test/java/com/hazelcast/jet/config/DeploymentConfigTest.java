package com.hazelcast.jet.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import java.io.File;
import java.net.URL;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.impl.job.deployment.DeploymentType.CLASS;
import static com.hazelcast.jet.impl.job.deployment.DeploymentType.DATA;
import static com.hazelcast.jet.impl.job.deployment.DeploymentType.JAR;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
@Ignore
public class DeploymentConfigTest {

    @Test
    public void testAddClass_with_Class() throws Exception {
        JobConfig config = new JobConfig();
        config.addClass(this.getClass());
        DeploymentConfig deploymentConfig = config.getDeploymentConfigs().iterator().next();

        assertEquals(this.getClass().getName(), deploymentConfig.getDescriptor().getId());
        assertEquals(CLASS, deploymentConfig.getDescriptor().getDeploymentType());
    }

    @Test
    public void testAddJar_with_Url() throws Exception {
        JobConfig config = new JobConfig();
        String urlString = "file://path/to/jarfile";
        config.addJar(new URL(urlString));
        DeploymentConfig deploymentConfig = config.getDeploymentConfigs().iterator().next();

        assertEquals("jarfile", deploymentConfig.getDescriptor().getId());
        assertEquals(JAR, deploymentConfig.getDescriptor().getDeploymentType());
        assertEquals(urlString, deploymentConfig.getUrl().toString());
    }

    @Test
    public void testAddJar_with_Url_and_JarName() throws Exception {
        JobConfig config = new JobConfig();
        String jarName = "jarFileName";
        String urlString = "file://path/to/jarfile";
        config.addJar(new URL(urlString), jarName);
        DeploymentConfig deploymentConfig = config.getDeploymentConfigs().iterator().next();

        assertEquals(jarName, deploymentConfig.getDescriptor().getId());
        assertEquals(JAR, deploymentConfig.getDescriptor().getDeploymentType());
        assertEquals(urlString, deploymentConfig.getUrl().toString());
    }

    @Test
    public void testAddJar_with_Path() throws Exception {
        JobConfig config = new JobConfig();
        String path = "/path/to/jarfile";
        config.addJar(path);
        DeploymentConfig deploymentConfig = config.getDeploymentConfigs().iterator().next();

        assertEquals("jarfile", deploymentConfig.getDescriptor().getId());
        assertEquals(JAR, deploymentConfig.getDescriptor().getDeploymentType());
        assertEquals(path, deploymentConfig.getUrl().getPath());
    }

    @Test
    public void testAddJar_with_Path_and_JarName() throws Exception {
        JobConfig config = new JobConfig();
        String jarName = "jarFileName";
        String path = "/path/to/jarfile";
        config.addJar(path, jarName);
        DeploymentConfig deploymentConfig = config.getDeploymentConfigs().iterator().next();

        assertEquals(jarName, deploymentConfig.getDescriptor().getId());
        assertEquals(JAR, deploymentConfig.getDescriptor().getDeploymentType());
        assertEquals(path, deploymentConfig.getUrl().getPath());
    }

    @Test
    public void testAddJar_with_File() throws Exception {
        JobConfig config = new JobConfig();
        File file = new File("/path/to/jarfile");
        config.addJar(file);
        DeploymentConfig deploymentConfig = config.getDeploymentConfigs().iterator().next();

        assertEquals("jarfile", deploymentConfig.getDescriptor().getId());
        assertEquals(JAR, deploymentConfig.getDescriptor().getDeploymentType());
        assertEquals(file.getPath(), deploymentConfig.getUrl().getPath());
    }

    @Test
    public void testAddJar_with_File_and_JarName() throws Exception {
        JobConfig config = new JobConfig();
        String jarName = "jarFileName";
        File file = new File("/path/to/jarfile");
        config.addJar(file, jarName);
        DeploymentConfig deploymentConfig = config.getDeploymentConfigs().iterator().next();

        assertEquals(jarName, deploymentConfig.getDescriptor().getId());
        assertEquals(JAR, deploymentConfig.getDescriptor().getDeploymentType());
        assertEquals(file.getPath(), deploymentConfig.getUrl().getPath());
    }

    @Test
    public void testAddResource_with_Url() throws Exception {
        JobConfig config = new JobConfig();
        String urlString = "file://path/to/resourceFile";
        config.addResource(new URL(urlString));
        DeploymentConfig deploymentConfig = config.getDeploymentConfigs().iterator().next();

        assertEquals("resourceFile", deploymentConfig.getDescriptor().getId());
        assertEquals(DATA, deploymentConfig.getDescriptor().getDeploymentType());
        assertEquals(urlString, deploymentConfig.getUrl().toString());
    }

    @Test
    public void testAddResource_with_Url_and_ResourceName() throws Exception {
        JobConfig config = new JobConfig();
        String resourceName = "resourceFileName";
        String urlString = "file://path/to/resourceFile";
        config.addResource(new URL(urlString), resourceName);
        DeploymentConfig deploymentConfig = config.getDeploymentConfigs().iterator().next();

        assertEquals(resourceName, deploymentConfig.getDescriptor().getId());
        assertEquals(DATA, deploymentConfig.getDescriptor().getDeploymentType());
        assertEquals(urlString, deploymentConfig.getUrl().toString());
    }

    @Test
    public void testAddResource_with_Path() throws Exception {
        JobConfig config = new JobConfig();
        String path = "/path/to/resource";
        config.addResource(path);
        DeploymentConfig deploymentConfig = config.getDeploymentConfigs().iterator().next();

        assertEquals("resource", deploymentConfig.getDescriptor().getId());
        assertEquals(DATA, deploymentConfig.getDescriptor().getDeploymentType());
        assertEquals(path, deploymentConfig.getUrl().getPath());
    }

    @Test
    public void testAddResource_with_Path_and_ResourceName() throws Exception {
        JobConfig config = new JobConfig();
        String resourceName = "resourceFileName";
        String path = "/path/to/jarfile";
        config.addResource(path, resourceName);
        DeploymentConfig deploymentConfig = config.getDeploymentConfigs().iterator().next();

        assertEquals(resourceName, deploymentConfig.getDescriptor().getId());
        assertEquals(DATA, deploymentConfig.getDescriptor().getDeploymentType());
        assertEquals(path, deploymentConfig.getUrl().getPath());
    }

    @Test
    public void testAddResource_with_File() throws Exception {
        JobConfig config = new JobConfig();
        File file = new File("/path/to/resource");
        config.addResource(file);
        DeploymentConfig deploymentConfig = config.getDeploymentConfigs().iterator().next();

        assertEquals("resource", deploymentConfig.getDescriptor().getId());
        assertEquals(DATA, deploymentConfig.getDescriptor().getDeploymentType());
        assertEquals(file.getPath(), deploymentConfig.getUrl().getPath());
    }

    @Test
    public void testAddResource_with_File_and_ResourceName() throws Exception {
        JobConfig config = new JobConfig();
        String resourceName = "resourceFileName";
        File file = new File("/path/to/resource");
        config.addResource(file, resourceName);
        DeploymentConfig deploymentConfig = config.getDeploymentConfigs().iterator().next();

        assertEquals(resourceName, deploymentConfig.getDescriptor().getId());
        assertEquals(DATA, deploymentConfig.getDescriptor().getDeploymentType());
        assertEquals(file.getPath(), deploymentConfig.getUrl().getPath());
    }


}
