package com.hazelcast.client.usercodedeployment;

import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.client.spi.impl.ClientUserCodeDeploymentService;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import usercodedeployment.IncrementingEntryProcessor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientUserCodeDeploymentConfigTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() throws Exception {
        factory.terminateAll();
    }

    @Test
    public void testConfigWithClassName() throws IOException {
        ClientUserCodeDeploymentConfig config = new ClientUserCodeDeploymentConfig();
        config.setEnabled(true);
        String className = "usercodedeployment.IncrementingEntryProcessor";
        config.addClass(className);
        ClientUserCodeDeploymentService service = new ClientUserCodeDeploymentService(config, this.getClass().getClassLoader());
        service.start();
        List<Map.Entry<String, byte[]>> list = service.getClassDefinitionList();
        assertClassLoaded(list, className);
    }

    @Test
    public void testConfigWithClass() throws IOException {
        ClientUserCodeDeploymentConfig config = new ClientUserCodeDeploymentConfig();
        config.setEnabled(true);
        config.addClass(IncrementingEntryProcessor.class);
        ClientUserCodeDeploymentService service = new ClientUserCodeDeploymentService(config, this.getClass().getClassLoader());
        service.start();
        List<Map.Entry<String, byte[]>> list = service.getClassDefinitionList();
        assertClassLoaded(list, IncrementingEntryProcessor.class.getName());
    }

    @Test
    public void testConfigWithJarPath() throws IOException {
        ClientUserCodeDeploymentConfig config = new ClientUserCodeDeploymentConfig();
        config.setEnabled(true);
        ClassLoader classLoader = getClass().getClassLoader();
        config.addJar("IncrementingEntryProcessor.jar");
        ClientUserCodeDeploymentService service = new ClientUserCodeDeploymentService(config, classLoader);
        service.start();
        List<Map.Entry<String, byte[]>> list = service.getClassDefinitionList();
        assertClassLoaded(list, "usercodedeployment.IncrementingEntryProcessor");
    }

    @Test(expected = FileNotFoundException.class)
    public void testConfigWithWrongJarPath() throws IOException {
        ClientUserCodeDeploymentConfig config = new ClientUserCodeDeploymentConfig();
        config.setEnabled(true);
        ClassLoader classLoader = getClass().getClassLoader();
        config.addJar("/wrongPath/IncrementingEntryProcessor.jar");
        ClientUserCodeDeploymentService service = new ClientUserCodeDeploymentService(config, classLoader);
        service.start();
    }

    @Test(expected = FileNotFoundException.class)
    public void testConfigWithFileDoesNotExist() throws IOException {
        ClientUserCodeDeploymentConfig config = new ClientUserCodeDeploymentConfig();
        config.setEnabled(true);
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File("/wrongPath/IncrementingEntryProcessor.jar");
        config.addJar(file);
        ClientUserCodeDeploymentService service = new ClientUserCodeDeploymentService(config, classLoader);
        service.start();
    }

    @Test
    public void testConfigWithJarFile() throws IOException, URISyntaxException {
        ClientUserCodeDeploymentConfig config = new ClientUserCodeDeploymentConfig();
        config.setEnabled(true);
        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource("IncrementingEntryProcessor.jar");
        File file = new File(resource.toURI());
        config.addJar(file);
        ClientUserCodeDeploymentService service = new ClientUserCodeDeploymentService(config, classLoader);
        service.start();
        List<Map.Entry<String, byte[]>> list = service.getClassDefinitionList();
        assertClassLoaded(list, "usercodedeployment.IncrementingEntryProcessor");
    }

    @Test
    public void testConfigWithJarFile_withInnerAndAnonymousClass() throws IOException, URISyntaxException {
        ClientUserCodeDeploymentConfig config = new ClientUserCodeDeploymentConfig();
        config.setEnabled(true);
        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource("EntryProcessorWithAnonymousAndInner.jar");
        File file = new File(resource.toURI());
        config.addJar(file);
        ClientUserCodeDeploymentService service = new ClientUserCodeDeploymentService(config, classLoader);
        service.start();
        List<Map.Entry<String, byte[]>> list = service.getClassDefinitionList();
        assertClassLoaded(list, "usercodedeployment.EntryProcessorWithAnonymousAndInner");
        assertClassLoaded(list, "usercodedeployment.EntryProcessorWithAnonymousAndInner$1");
        assertClassLoaded(list, "usercodedeployment.EntryProcessorWithAnonymousAndInner$Test");
    }

    private void assertClassLoaded(List<Map.Entry<String, byte[]>> list, String name) {
        for (Map.Entry<String, byte[]> classDefinition : list) {
            if (classDefinition.getKey().equals(name)) {
                return;
            }
        }
        fail();
    }

}
