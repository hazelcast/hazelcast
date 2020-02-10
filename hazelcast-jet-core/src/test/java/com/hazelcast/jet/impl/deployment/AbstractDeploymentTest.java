/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractDeploymentTest extends SimpleTestInClusterSupport {

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
        URL classUrl = this.getClass().getResource("/cp1/");
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[] {classUrl}, null);
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
        URL classUrl = this.getClass().getResource("/cp1/");
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[] {classUrl}, null);
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
        Pipeline pipeline = Pipeline.create();
        String fileToAttach = Paths.get(getClass().getResource("/deployment/resource.txt").toURI()).toString();

        pipeline.readFrom(TestSources.items(1))
                .mapUsingService(ServiceFactory.withCreateContextFn(context -> context.attachedFile(fileToAttach))
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

        JetInstance jetInstance = getJetInstance();
        JobConfig jobConfig = new JobConfig();
        jobConfig.attachFile(fileToAttach, fileToAttach);

        executeAndPeel(jetInstance.newJob(pipeline, jobConfig));
    }

    @Test
    public void testDeployment_whenAttachDirectory_thenFilesAvailableOnMembers() throws Throwable {
        Pipeline pipeline = Pipeline.create();
        String dirToAttach = Paths.get(this.getClass().getResource("/deployment").toURI()).toString();

        pipeline.readFrom(TestSources.items(1))
                .flatMapUsingService(
                        ServiceFactories.sharedService(context -> context.attachedDirectory(dirToAttach)),
                        (file, integer) -> Traversers.traverseStream(Files.list(file.toPath()).map(Path::toString)))
                .apply(assertCollected(c -> {
                    c.forEach(s -> assertTrue(new File(s).exists()));
                    assertEquals("list size must be 3", 3, c.size());
                }))
                .writeTo(Sinks.logger());

        JetInstance jetInstance = getJetInstance();
        JobConfig jobConfig = new JobConfig();
        jobConfig.attachDirectory(dirToAttach, dirToAttach);

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
