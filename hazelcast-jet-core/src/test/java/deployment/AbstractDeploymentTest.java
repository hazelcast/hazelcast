/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package deployment;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.JobConfig;
import com.hazelcast.jet.Vertex;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.util.FilteringClassLoader;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import org.junit.Test;

import static com.hazelcast.jet.TestUtil.executeAndPeel;

public abstract class AbstractDeploymentTest extends HazelcastTestSupport {

    protected abstract JetInstance getJetInstance();

    protected abstract void createCluster();

    @Test
    public void test_Jar_Distribution() throws Throwable {
        createCluster();

        DAG dag = new DAG();
        dag.vertex(new Vertex("create and print person", LoadPersonIsolated::new));


        JetInstance jetInstance = getJetInstance();
        JobConfig jobConfig = new JobConfig();
        jobConfig.addJar(this.getClass().getResource("/sample-pojo-1.0-person.jar"));
        jobConfig.addJar(this.getClass().getResource("/sample-pojo-1.0-deployment.jar"));
        jobConfig.addClass(AbstractDeploymentTest.class);

        executeAndPeel(jetInstance.newJob(dag, jobConfig));
    }

    @Test
    public void test_Class_Distribution() throws Throwable {
        createCluster();

        DAG dag = new DAG();
        dag.vertex(new Vertex("create and print person", LoadPersonIsolated::new));

        JobConfig jobConfig = new JobConfig();
        URL classUrl = this.getClass().getResource("/cp1/");
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{classUrl}, null);
        Class<?> appearance = urlClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");
        jobConfig.addClass(appearance);
        jobConfig.addJar(this.getClass().getResource("/sample-pojo-1.0-deployment.jar"));
        jobConfig.addClass(AbstractDeploymentTest.class);

        executeAndPeel(getJetInstance().newJob(dag, jobConfig));
    }

    protected Object createIsolatedNode(Thread thread, FilteringClassLoader cl) throws Exception {
        thread.setContextClassLoader(cl);
        Class<?> jetConfigClazz = cl.loadClass("com.hazelcast.jet.JetConfig");
        Class<?> hazelcastConfigClazz = cl.loadClass("com.hazelcast.config.Config");
        Object config = jetConfigClazz.newInstance();
        Method getHazelcastConfig = jetConfigClazz.getDeclaredMethod("getHazelcastConfig");
        Object hazelcastConfig = getHazelcastConfig.invoke(config);
        Method setClassLoader = hazelcastConfigClazz.getDeclaredMethod("setClassLoader", ClassLoader.class);
        setClassLoader.invoke(hazelcastConfig, cl);

        Class<?> jetClazz = cl.loadClass("com.hazelcast.jet.Jet");
        Method newJetInstance = jetClazz.getDeclaredMethod("newJetInstance", jetConfigClazz);
        return newJetInstance.invoke(jetClazz, config);
    }
}
