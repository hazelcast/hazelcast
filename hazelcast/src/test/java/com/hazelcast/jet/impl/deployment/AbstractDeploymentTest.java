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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.config.JobClassLoaderFactory;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.deployment.LoadResource.LoadResourceMetaSupplier;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.TestUtil.executeAndPeel;
import static com.hazelcast.jet.pipeline.test.Assertions.assertCollected;
import static java.util.Collections.emptyEnumeration;
import static java.util.Collections.enumeration;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class AbstractDeploymentTest extends SimpleTestInClusterSupport {

    public static final String CLASS_LOADER_PREFIX = "/cp1/";

    protected abstract JetInstance getJetInstance();

    @Test
    public void testDeployment_whenJarAddedAsResource_thenClassesAvailableOnClassLoader() throws Throwable {
        DAG dag = new DAG();
        dag.newVertex("load class", () -> new LoadClassesIsolated(true));

        JetInstance jetInstance = getJetInstance();
        JobConfig jobConfig = new JobConfig();
        jobConfig.addJar(this.getClass().getResource("/deployment/sample-pojo-1.0-person.jar"));

        executeAndPeel(jetInstance.newJob(dag, jobConfig));
    }

    @Test
    public void testDeployment_whenClassAddedAsResource_thenClassAvailableOnClassLoader() throws Throwable {
        DAG dag = new DAG();
        dag.newVertex("create and print person", () -> new LoadClassesIsolated(true));

        JobConfig jobConfig = new JobConfig();
        URL classUrl = this.getClass().getResource(CLASS_LOADER_PREFIX);
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{classUrl}, null);
        Class<?> appearance = urlClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");
        jobConfig.addClass(appearance);

        executeAndPeel(getJetInstance().newJob(dag, jobConfig));
    }

    @Test
    public void testDeployment_whenClassAddedAsResource_then_availableInDestroyWhenCancelled() throws Throwable {
        DAG dag = new DAG();
        LoadClassesIsolated.assertionErrorInClose = null;
        dag.newVertex("v", () -> new LoadClassesIsolated(false));

        JobConfig jobConfig = new JobConfig();
        URL classUrl = this.getClass().getResource(CLASS_LOADER_PREFIX);
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{classUrl}, null);
        Class<?> appearanceClz = urlClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");
        jobConfig.addClass(appearanceClz);

        Job job = getJetInstance().newJob(dag, jobConfig);
        assertJobStatusEventually(job, RUNNING);
        cancelAndJoin(job);
        if (LoadClassesIsolated.assertionErrorInClose != null) {
            throw LoadClassesIsolated.assertionErrorInClose;
        }
    }

    @Test
    public void testDeployment_whenAddClass_thenNestedClassesAreAddedAsWell() throws Throwable {
        DAG dag = new DAG();
        dag.newVertex("executes lambda from a nested class", NestedClassIsLoaded::new);

        JobConfig jobConfig = new JobConfig();
        URL classUrl = this.getClass().getResource("/cp1/");
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{classUrl}, null);
        Class<?> worker = urlClassLoader.loadClass("com.sample.lambda.Worker");
        jobConfig.addClass(worker);

        executeAndPeel(getJetInstance().newJob(dag, jobConfig));
    }

    @Test
    public void testDeployment_when_customClassLoaderFactory_then_used() throws Throwable {
        DAG dag = new DAG();
        dag.newVertex("load resource", new LoadResourceMetaSupplier());

        JobConfig jobConfig = new JobConfig();
        jobConfig.setClassLoaderFactory(new MyJobClassLoaderFactory());

        executeAndPeel(getJetInstance().newJob(dag, jobConfig));
    }

    @Test
    public void testDeployment_whenZipAddedAsResource_thenClassesAvailableOnClassLoader() throws Throwable {
        DAG dag = new DAG();
        dag.newVertex("load class", () -> new LoadClassesIsolated(true));

        JetInstance jetInstance = getJetInstance();
        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(this.getClass().getResource("/zip-resources/person-jar.zip"));

        executeAndPeel(jetInstance.newJob(dag, jobConfig));
    }

    @Test
    public void testDeployment_whenZipAddedAsResource_thenClassesFromAllJarsAvailableOnClassLoader() throws Throwable {
        DAG dag = new DAG();
        List<String> onClasspath = new ArrayList<>();
        onClasspath.add("com.sample.pojo.person.Person$Appereance");
        onClasspath.add("com.sample.pojo.car.Car");
        List<String> notOnClasspath = new ArrayList<>();
        notOnClasspath.add("com.sample.pojo.address.Address");
        dag.newVertex("load class", () -> new LoadClassesIsolated(onClasspath, notOnClasspath, true));

        JetInstance jetInstance = getJetInstance();
        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(this.getClass().getResource("/zip-resources/person-car-jar.zip"));

        executeAndPeel(jetInstance.newJob(dag, jobConfig));
    }

    @Test
    public void testDeployment_whenAttachFile_thenFileAvailableOnMembers() throws Throwable {
        String fileToAttach = Paths.get(getClass().getResource("/deployment/resource.txt").toURI()).toString();

        Pipeline pipeline = attachFilePipeline(fileToAttach);

        JetInstance jetInstance = getJetInstance();
        JobConfig jobConfig = new JobConfig();
        jobConfig.attachFile(fileToAttach, fileToAttach);

        executeAndPeel(jetInstance.newJob(pipeline, jobConfig));
    }

    @Test
    public void testDeployment_whenAttachFileWithoutId_thenFileAvailableOnMembers() throws Throwable {
        String fileName = "resource.txt";
        String fileToAttach = Paths.get(getClass().getResource("/deployment/" + fileName).toURI()).toString();

        Pipeline pipeline = attachFilePipeline(fileName);

        JetInstance jetInstance = getJetInstance();
        JobConfig jobConfig = new JobConfig();
        jobConfig.attachFile(fileToAttach);

        executeAndPeel(jetInstance.newJob(pipeline, jobConfig));
    }

    private Pipeline attachFilePipeline(String attachedFile) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(1))
                .mapUsingService(ServiceFactory.withCreateContextFn(context -> context.attachedFile(attachedFile))
                                               .withCreateServiceFn((context, file) -> file),
                        (file, integer) -> {
                            assertTrue("File does not exist", file.exists());
                            assertEquals("resource.txt", file.getName());
                            boolean containsData = Files.readAllLines(file.toPath())
                                                        .stream()
                                                        .findFirst()
                                                        .orElseThrow(() -> new AssertionError("File is empty"))
                                                        .startsWith("AAAP");
                            assertTrue(containsData);
                            return file;
                        })
                .writeTo(Sinks.logger());
        return pipeline;
    }

    @Test
    public void testDeployment_whenAttachDirectory_thenFilesAvailableOnMembers() throws Throwable {
        String dirToAttach = Paths.get(this.getClass().getResource("/deployment").toURI()).toString();

        Pipeline pipeline = attachDirectoryPipeline(dirToAttach);

        JetInstance jetInstance = getJetInstance();
        JobConfig jobConfig = new JobConfig();
        jobConfig.attachDirectory(dirToAttach, dirToAttach);

        executeAndPeel(jetInstance.newJob(pipeline, jobConfig));
    }

    @Test
    public void testDeployment_whenAttachDirectoryWithoutId_thenFilesAvailableOnMembers() throws Throwable {
        String dirName = "deployment";
        String dirToAttach = Paths.get(this.getClass().getResource("/" + dirName).toURI()).toString();

        Pipeline pipeline = attachDirectoryPipeline(dirName);

        JetInstance jetInstance = getJetInstance();
        JobConfig jobConfig = new JobConfig();
        jobConfig.attachDirectory(dirToAttach);

        executeAndPeel(jetInstance.newJob(pipeline, jobConfig));
    }

    private Pipeline attachDirectoryPipeline(String attachedDirectory) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(1))
                .flatMapUsingService(
                        ServiceFactories.sharedService(context -> context.attachedDirectory(attachedDirectory)),
                        (file, integer) -> Traversers.traverseStream(Files.list(file.toPath()).map(Path::toString)))
                .apply(assertCollected(c -> {
                    c.forEach(s -> assertTrue(new File(s).exists()));
                    assertEquals("list size must be 3", 3, c.size());
                }))
                .writeTo(Sinks.logger());
        return pipeline;
    }

    @Test
    public void testDeployment_whenAttachNestedDirectory_thenFilesAvailableOnMembers() throws Throwable {
        Pipeline pipeline = Pipeline.create();
        String dirName = "nested";
        String dirToAttach = Paths.get(this.getClass().getResource("/" + dirName).toURI()).toString();

        pipeline.readFrom(TestSources.items(1))
                .flatMapUsingService(
                        ServiceFactories.sharedService(context -> context.attachedDirectory(dirName)),
                        (file, integer) -> Traversers.traverseStream(Files.list(file.toPath()).map(Path::toString)))
                .apply(assertCollected(c -> {
                    c.forEach(s -> {
                        File dir = new File(s);
                        assertTrue(dir.exists());
                        try {
                            List<Path> subFiles = Files.list(dir.toPath()).collect(toList());
                            assertEquals("each dir should contain 1 file", 1, subFiles.size());
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }
                    });
                    assertEquals("list size must be 3", 3, c.size());
                }))
                .writeTo(Sinks.logger());

        JetInstance jetInstance = getJetInstance();
        JobConfig jobConfig = new JobConfig();
        jobConfig.attachDirectory(dirToAttach);

        executeAndPeel(jetInstance.newJob(pipeline, jobConfig));
    }

    @Test
    public void testDeployment_whenAttachMoreFilesAndDirs_thenAllAvailableOnMembers() throws Throwable {
        Pipeline pipeline = Pipeline.create();
        String dirToAttach1 = Paths.get(this.getClass().getResource("/nested/folder").toURI()).toString();
        String dirToAttach2 = Paths.get(this.getClass().getResource("/nested/folder1").toURI()).toString();
        String fileToAttach1 = Paths.get(getClass().getResource("/deployment/resource.txt").toURI()).toString();
        String fileToAttach2 = Paths.get(getClass().getResource("/nested/folder2/test2").toURI()).toString();

        pipeline.readFrom(TestSources.items(1))
                .mapUsingService(
                        ServiceFactories.sharedService(context -> {
                            assertTrue(context.attachedDirectory(dirToAttach1).exists());
                            assertTrue(context.attachedDirectory(dirToAttach1).isDirectory());
                            assertTrue(context.attachedDirectory(dirToAttach2).exists());
                            assertTrue(context.attachedDirectory(dirToAttach2).isDirectory());
                            assertTrue(context.attachedFile(fileToAttach1).exists());
                            assertFalse(context.attachedFile(fileToAttach1).isDirectory());
                            assertTrue(context.attachedFile(fileToAttach2).exists());
                            assertFalse(context.attachedFile(fileToAttach2).isDirectory());
                            return true;
                        }),
                        (state, integer) -> state)
                .writeTo(Sinks.logger());

        JetInstance jetInstance = getJetInstance();
        JobConfig jobConfig = new JobConfig();
        jobConfig.attachDirectory(dirToAttach1, dirToAttach1);
        jobConfig.attachDirectory(dirToAttach2, dirToAttach2);
        jobConfig.attachFile(fileToAttach1, fileToAttach1);
        jobConfig.attachFile(fileToAttach2, fileToAttach2);

        executeAndPeel(jetInstance.newJob(pipeline, jobConfig));
    }

    @Test
    public void testDeployment_whenFileAddedAsResource_thenAvailableOnClassLoader() throws Throwable {
        DAG dag = new DAG();
        dag.newVertex("load resource", new LoadResourceMetaSupplier());

        JobConfig jobConfig = new JobConfig();
        jobConfig.addClasspathResource(this.getClass().getResource("/deployment/resource.txt"), "customId");

        executeAndPeel(getJetInstance().newJob(dag, jobConfig));
    }

    static class MyJobClassLoaderFactory implements JobClassLoaderFactory {

        @Nonnull
        @Override
        public ClassLoader getJobClassLoader() {
            return new ClassLoader() {
                @Override
                protected Enumeration<URL> findResources(String name) {
                    if (name.equals("customId")) {
                        return enumeration(singleton(this.getClass().getResource("/deployment/resource.txt")));
                    }
                    return emptyEnumeration();
                }

                @Override
                protected URL findResource(String name) {
                    // return first resource from findResources
                    Enumeration<URL> en = findResources(name);
                    return en.hasMoreElements() ? en.nextElement() : null;
                }
            };
        }
    }
}
