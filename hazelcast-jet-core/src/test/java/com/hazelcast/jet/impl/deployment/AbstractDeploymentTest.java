/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.config.JobClassLoaderFactory;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.deployment.LoadResource.LoadResourceMetaSupplier;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.function.Function;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.TestUtil.executeAndPeel;
import static java.util.Collections.emptyEnumeration;
import static java.util.Collections.enumeration;
import static java.util.Collections.singleton;

public abstract class AbstractDeploymentTest extends JetTestSupport {

    protected abstract JetInstance getJetInstance();

    protected abstract void createCluster();

    @Test
    public void testDeployment_whenJarAddedAsResource_thenClassesAvailableOnClassLoader() throws Throwable {
        createCluster();

        DAG dag = new DAG();
        dag.newVertex("load class", () -> new LoadPersonIsolated(true));

        JetInstance jetInstance = getJetInstance();
        JobConfig jobConfig = new JobConfig();
        jobConfig.addJar(this.getClass().getResource("/deployment/sample-pojo-1.0-person.jar"));

        executeAndPeel(jetInstance.newJob(dag, jobConfig));
    }

    @Test
    public void testDeployment_whenClassAddedAsResource_thenClassAvailableOnClassLoader() throws Throwable {
        createCluster();

        DAG dag = new DAG();
        dag.newVertex("create and print person", () -> new LoadPersonIsolated(true));

        JobConfig jobConfig = new JobConfig();
        URL classUrl = this.getClass().getResource("/cp1/");
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{classUrl}, null);
        Class<?> appearance = urlClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");
        jobConfig.addClass(appearance);

        executeAndPeel(getJetInstance().newJob(dag, jobConfig));
    }

    @Test
    public void testDeployment_whenClassAddedAsResource_then_availableInDestroyWhenCancelled() throws Throwable {
        createCluster();

        DAG dag = new DAG();
        LoadPersonIsolated.assertionErrorInClose = null;
        dag.newVertex("v", () -> new LoadPersonIsolated(false));

        JobConfig jobConfig = new JobConfig();
        URL classUrl = this.getClass().getResource("/cp1/");
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{classUrl}, null);
        Class<?> appearanceClz = urlClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");
        jobConfig.addClass(appearanceClz);

        Job job = getJetInstance().newJob(dag, jobConfig);
        assertJobStatusEventually(job, RUNNING);
        job.cancel();
        try {
            job.join();
        } catch (CancellationException ignored) {
        }
        if (LoadPersonIsolated.assertionErrorInClose != null) {
            throw LoadPersonIsolated.assertionErrorInClose;
        }
    }

    @Test
    public void testDeployment_whenFileAddedAsResource_thenAvailableOnClassLoader() throws Throwable {
        createCluster();

        DAG dag = new DAG();
        dag.newVertex("load resource", new LoadResourceMetaSupplier());

        JobConfig jobConfig = new JobConfig();
        jobConfig.addResource(this.getClass().getResource("/deployment/resource.txt"), "customId");

        executeAndPeel(getJetInstance().newJob(dag, jobConfig));
    }

    @Test
    public void testDeployment_when_customClassLoaderFactory_then_used() throws Throwable {
        createCluster();

        DAG dag = new DAG();
        dag.newVertex("load resource", new LoadResourceMetaSupplier());

        JobConfig jobConfig = new JobConfig();
        jobConfig.setClassLoaderFactory(new MyJobClassLoaderFactory());

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

    static class MyMapper implements Function<Map.Entry<Integer, Integer>, Integer>, Serializable {

        @Override
        public Integer apply(Map.Entry<Integer, Integer> entry) {
            return entry.getKey();
        }
    }
}
