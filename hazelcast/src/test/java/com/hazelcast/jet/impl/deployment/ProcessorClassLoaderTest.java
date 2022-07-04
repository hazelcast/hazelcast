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

package com.hazelcast.jet.impl.deployment;

import childfirstclassloader.TestProcessor;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.JarUtil;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.HazelcastAPIDelegatingClassloader;
import com.hazelcast.test.starter.HazelcastStarter;
import org.example.jet.impl.deployment.ResourceCollector;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.newArrayList;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProcessorClassLoaderTest extends JetTestSupport {

    private static final String SOURCE_NAME = "test-source";

    private HazelcastInstance member;
    private HazelcastInstance client;
    private JetService jet;

    private static File jarFile;
    private static File resourcesJarFile;

    @BeforeClass
    public static void beforeClass() throws Exception {
        jarFile = File.createTempFile("source_", ".jar");
        JarUtil.createJarFile(
                "target/test-classes/",
                newArrayList(
                        classToPath(TestProcessor.ResourceReader.class),
                        classToPath(TestProcessor.TestProcessorMetaSupplier.class),
                        classToPath(TestProcessor.TestProcessorSupplier.class),
                        classToPath(TestProcessor.class),
                        classToPath(SourceWithClassLoader.class)
                ),
                jarFile.getAbsolutePath()
        );
        System.out.println(jarFile);

        resourcesJarFile = File.createTempFile("resources_", ".jar");
        JarUtil.createResourcesJarFile(resourcesJarFile);

        // Setup the path for custom lib directory, this is by default set to `custom-lib` directory in hazelcast
        // distribution zip
        System.setProperty(ClusterProperty.PROCESSOR_CUSTOM_LIB_DIR.getName(), System.getProperty("java.io.tmpdir"));
    }

    private static String classToPath(Class<?> clazz) {
        return clazz.getName().replace(".", "/") + ".class";
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (jarFile != null) {
            jarFile.delete();
            jarFile = null;
        }
        if (resourcesJarFile != null) {
            resourcesJarFile.delete();
            resourcesJarFile = null;
        }
    }

    @Before
    public void setUp() throws Exception {
        ResourceCollector.items().clear();

        member = createHazelcastMember();
        client = HazelcastClient.newHazelcastClient();
        jet = client.getJet();
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.shutdown();
        }
        if (member != null) {
            member.shutdown();
        }
    }

    private HazelcastInstance createHazelcastMember() throws MalformedURLException {
        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);

        // Create a member in a separate classloader without the test classes loaded
        URL classesUrl = new File("target/classes/").toURI().toURL();
        HazelcastAPIDelegatingClassloader classloader = new HazelcastAPIDelegatingClassloader(
                new URL[]{classesUrl},
                // Need to delegate to system classloader, which has maven dependencies like Jackson
                ClassLoader.getSystemClassLoader()
        );
        return HazelcastStarter.newHazelcastInstance(config, classloader);
    }

    @Test
    public void testClassLoaderForBatchSource() throws Exception {
        Pipeline p = Pipeline.create();
        BatchSource<String> source = SourceWithClassLoader.batchSource(SOURCE_NAME);

        p.readFrom(source).setLocalParallelism(1)
         .writeTo(Sinks.list("test"));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addCustomClasspath(source.name(), resourcesJarFile.getName());
        jobConfig.addCustomClasspath(source.name(), jarFile.getName());
        jet.newJob(p, jobConfig).join();

        IList<Object> list = member.getList("test");
        assertThat(list).contains("resource in jar");

        assertThat(ResourceCollector.items()).containsExactly(
                "Processor init resource in jar",
                "Processor complete resource in jar"
        );
    }

    @Test
    public void testClassLoaderForStreamSource() throws Exception {
        Pipeline p = Pipeline.create();
        StreamSource<String> source = SourceWithClassLoader.streamSource(SOURCE_NAME);

        p.readFrom(source)
         .withoutTimestamps()
         .setLocalParallelism(1)
         .writeTo(Sinks.list("test"));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addCustomClasspath(source.name(), resourcesJarFile.getName());
        jobConfig.addCustomClasspath(source.name(), jarFile.getName());
        jet.newJob(p, jobConfig).join();

        IList<Object> list = member.getList("test");
        assertThat(list).contains("resource in jar");

        assertThat(ResourceCollector.items()).containsExactly(
                "Processor init resource in jar",
                "Processor complete resource in jar"
        );
    }

    @Test
    public void testClassLoaderSetForSupplierDAG() {
        DAG dag = new DAG();
        dag.newVertex(SOURCE_NAME, TestProcessor.TestProcessorMetaSupplier.create())
           .localParallelism(1);

        JobConfig jobConfig = new JobConfig();
        jobConfig.addCustomClasspath(SOURCE_NAME, resourcesJarFile.getName());
        jobConfig.addCustomClasspath(SOURCE_NAME, jarFile.getName());
        jet.newJob(dag, jobConfig).join();

        List<String> items = ResourceCollector.items();
        assertThat(items).containsExactly(
                "ProcessorMetaSupplier init resource in jar",
                "ProcessorMetaSupplier get resource in jar",
                "ProcessorMetaSupplier create resource in jar",

                "ProcessorSupplier init resource in jar",
                "ProcessorSupplier get resource in jar",

                "Processor init resource in jar",
                "Processor complete resource in jar",

                "ProcessorSupplier close resource in jar",
                "ProcessorMetaSupplier close resource in jar"
        );
    }

}
