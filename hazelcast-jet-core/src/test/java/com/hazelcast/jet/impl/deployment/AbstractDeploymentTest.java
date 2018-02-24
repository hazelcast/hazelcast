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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.deployment.LoadPersonIsolated.LoadPersonIsolatedMetaSupplier;
import com.hazelcast.jet.impl.deployment.LoadResource.LoadResourceMetaSupplier;
import com.hazelcast.jet.stream.DistributedStream;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.test.IgnoredForCoverage;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.hazelcast.jet.core.TestUtil.executeAndPeel;
import static com.hazelcast.jet.stream.DistributedCollectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;

public abstract class AbstractDeploymentTest extends HazelcastTestSupport {

    protected abstract JetInstance getJetInstance();

    protected abstract void createCluster();

    @Test
    public void testDeployment_whenJarAddedAsResource_thenClassesAvailableOnClassLoader() throws Throwable {
        createCluster();

        DAG dag = new DAG();
        dag.newVertex("load class", new LoadPersonIsolatedMetaSupplier());

        JetInstance jetInstance = getJetInstance();
        JobConfig jobConfig = new JobConfig();
        jobConfig.addJar(this.getClass().getResource("/deployment/sample-pojo-1.0-person.jar"));

        executeAndPeel(jetInstance.newJob(dag, jobConfig));
    }

    /**
     * The test is excluded from the test coverage since JaCoCo instrumentation messes up with the
     * class internals which generates different serialVersionUid's for same classes.
     * This leads test to fail with InvalidClassException
     */
    @Test
    @Category(IgnoredForCoverage.class)
    public void testStream() throws Throwable {
        createCluster();

        IMapJet<Integer, Integer> map = getJetInstance().getMap(randomString());
        range(0, 10).parallel().forEach(i -> map.put(i, i));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(MyMapper.class);
        List<Integer> list = DistributedStream
                .fromMap(map)
                .configure(jobConfig)
                .map(new MyMapper())
                .collect(toList());
        assertEquals(10, list.size());
    }

    @Test
    public void testDeployment_whenClassAddedAsResource_thenClassAvailableOnClassLoader() throws Throwable {
        createCluster();

        DAG dag = new DAG();
        dag.newVertex("create and print person", new LoadPersonIsolatedMetaSupplier());

        JobConfig jobConfig = new JobConfig();
        URL classUrl = this.getClass().getResource("/cp1/");
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{classUrl}, null);
        Class<?> appearance = urlClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");
        jobConfig.addClass(appearance);

        executeAndPeel(getJetInstance().newJob(dag, jobConfig));
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

    static class MyMapper implements Function<Map.Entry<Integer, Integer>, Integer>, Serializable {

        @Override
        public Integer apply(Map.Entry<Integer, Integer> entry) {
            return entry.getKey();
        }
    }
}
