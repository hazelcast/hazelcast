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

package com.hazelcast.client.usercodedeployment;

import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.client.impl.spi.impl.ClientUserCodeDeploymentService;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import usercodedeployment.IncrementingEntryProcessor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientUserCodeDeploymentConfigTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() throws Exception {
        factory.terminateAll();
    }

    @Test
    public void testConfigWithClassName() throws IOException, ClassNotFoundException {
        ClientUserCodeDeploymentConfig config = new ClientUserCodeDeploymentConfig();
        config.setEnabled(true);
        String className = "usercodedeployment.IncrementingEntryProcessor";
        config.addClass(className);
        ClientUserCodeDeploymentService service = new ClientUserCodeDeploymentService(config, this.getClass().getClassLoader());
        service.start();
        List<Map.Entry<String, byte[]>> list = service.getClassDefinitionList();
        assertClassLoaded(list, className);
    }

    @Test(expected = ClassNotFoundException.class)
    public void testConfigWithWrongClassName() throws ClassNotFoundException, IOException {
        ClientUserCodeDeploymentConfig config = new ClientUserCodeDeploymentConfig();
        config.setEnabled(true);
        String className = "NonExistingClass";
        config.addClass(className);
        ClientUserCodeDeploymentService service = new ClientUserCodeDeploymentService(config, this.getClass().getClassLoader());
        service.start();
    }

    @Test
    public void testConfigWithClass() throws ClassNotFoundException, IOException {
        ClientUserCodeDeploymentConfig config = new ClientUserCodeDeploymentConfig();
        config.setEnabled(true);
        config.addClass(IncrementingEntryProcessor.class);
        ClientUserCodeDeploymentService service = new ClientUserCodeDeploymentService(config, this.getClass().getClassLoader());
        service.start();
        List<Map.Entry<String, byte[]>> list = service.getClassDefinitionList();
        assertClassLoaded(list, IncrementingEntryProcessor.class.getName());
    }

    @Test
    public void testConfigWithJarPath() throws ClassNotFoundException, IOException {
        ClientUserCodeDeploymentConfig config = new ClientUserCodeDeploymentConfig();
        config.setEnabled(true);
        ClassLoader classLoader = getClass().getClassLoader();
        config.addJar("IncrementingEntryProcessor.jar");
        ClientUserCodeDeploymentService service = new ClientUserCodeDeploymentService(config, classLoader);
        service.start();
        List<Map.Entry<String, byte[]>> list = service.getClassDefinitionList();
        assertClassLoaded(list, "usercodedeployment.IncrementingEntryProcessor");
    }

    @Test
    public void testConfigWithClasses() throws ClassNotFoundException, IOException {
        ClientUserCodeDeploymentConfig config = new ClientUserCodeDeploymentConfig();
        config.setEnabled(true);
        config.setClassNames(Collections.singletonList("usercodedeployment.IncrementingEntryProcessor"));
        ClientUserCodeDeploymentService service = new ClientUserCodeDeploymentService(config, this.getClass().getClassLoader());
        service.start();
        List<Map.Entry<String, byte[]>> list = service.getClassDefinitionList();
        assertClassLoaded(list, IncrementingEntryProcessor.class.getName());
    }

    @Test
    public void testConfigWithJarPaths() throws ClassNotFoundException, IOException {
        ClientUserCodeDeploymentConfig config = new ClientUserCodeDeploymentConfig();
        config.setEnabled(true);
        ClassLoader classLoader = getClass().getClassLoader();
        config.setJarPaths(Collections.singletonList("IncrementingEntryProcessor.jar"));
        ClientUserCodeDeploymentService service = new ClientUserCodeDeploymentService(config, classLoader);
        service.start();
        List<Map.Entry<String, byte[]>> list = service.getClassDefinitionList();
        assertClassLoaded(list, "usercodedeployment.IncrementingEntryProcessor");
    }


    @Test
    public void testConfigWithURLPath() throws ClassNotFoundException, IOException {
        ClientUserCodeDeploymentConfig config = new ClientUserCodeDeploymentConfig();
        config.setEnabled(true);
        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource("IncrementingEntryProcessor.jar");
        config.addJar(resource.toExternalForm());
        ClientUserCodeDeploymentService service = new ClientUserCodeDeploymentService(config, classLoader);
        service.start();
        List<Map.Entry<String, byte[]>> list = service.getClassDefinitionList();
        assertClassLoaded(list, "usercodedeployment.IncrementingEntryProcessor");
    }

    @Test(expected = FileNotFoundException.class)
    public void testConfigWithWrongJarPath() throws ClassNotFoundException, IOException {
        ClientUserCodeDeploymentConfig config = new ClientUserCodeDeploymentConfig();
        config.setEnabled(true);
        ClassLoader classLoader = getClass().getClassLoader();
        config.addJar("/wrongPath/IncrementingEntryProcessor.jar");
        ClientUserCodeDeploymentService service = new ClientUserCodeDeploymentService(config, classLoader);
        service.start();
    }

    @Test(expected = FileNotFoundException.class)
    public void testConfigWithFileDoesNotExist() throws ClassNotFoundException, IOException {
        ClientUserCodeDeploymentConfig config = new ClientUserCodeDeploymentConfig();
        config.setEnabled(true);
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File("/wrongPath/IncrementingEntryProcessor.jar");
        config.addJar(file);
        ClientUserCodeDeploymentService service = new ClientUserCodeDeploymentService(config, classLoader);
        service.start();
    }

    @Test
    public void testConfigWithJarFile() throws URISyntaxException, ClassNotFoundException, IOException {
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

    private void assertClassLoaded(List<Map.Entry<String, byte[]>> list, String name) {
        for (Map.Entry<String, byte[]> classDefinition : list) {
            if (classDefinition.getKey().equals(name)) {
                return;
            }
        }
        fail();
    }

    @Test
    public void testConfigWithJarFile_withInnerAndAnonymousClass() throws IOException, URISyntaxException, ClassNotFoundException {
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
        assertClassLoaded(list, "usercodedeployment.EntryProcessorWithAnonymousAndInner$Test");
    }

    private static class CustomClassLoader extends ClassLoader {

        boolean getResourceAsStreamCalled;

        @Override
        public InputStream getResourceAsStream(String name) {
            getResourceAsStreamCalled = true;
            return super.getResourceAsStream(name);
        }
    }

    @Test
    public void testUserCodeDeploymentUsesCurrentThreadContextClassLoader() throws ClassNotFoundException, IOException {
        ClientUserCodeDeploymentConfig config = new ClientUserCodeDeploymentConfig();
        CustomClassLoader classLoader = new CustomClassLoader();

        config.setEnabled(true);
        config.addClass(IncrementingEntryProcessor.class);

        Thread.currentThread().setContextClassLoader(classLoader);
        ClientUserCodeDeploymentService service = new ClientUserCodeDeploymentService(config, null);
        service.start();
        List<Map.Entry<String, byte[]>> list = service.getClassDefinitionList();
        assertClassLoaded(list, IncrementingEntryProcessor.class.getName());

        assertTrue(classLoader.getResourceAsStreamCalled);
    }

}
