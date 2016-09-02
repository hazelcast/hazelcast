package com.hazelcast.jet.impl.job.deployment;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetEngine;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.impl.job.deployment.processors.ApacheV1;
import com.hazelcast.jet.impl.job.deployment.processors.ApacheV2;
import com.hazelcast.jet.impl.job.deployment.processors.PrintCarVertex;
import com.hazelcast.jet.impl.job.deployment.processors.PrintPersonVertex;
import com.hazelcast.jet.Job;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.Future;
import org.junit.Test;

public abstract class AbstractDeploymentTest extends JetTestSupport {

    abstract TestHazelcastInstanceFactory getFactory();

    abstract HazelcastInstance getHazelcastInstance();

    @Test
    public void test_Jar_Distribution() throws Exception {
        getFactory().newInstances(new Config(), 3);

        DAG dag = new DAG();
        dag.addVertex(createVertex("create and print person", PrintPersonVertex.class));

        String name = generateRandomString(10);
        JobConfig jobConfig = new JobConfig(name);
        jobConfig.addJar(this.getClass().getResource("/sample-pojo-1.0-person.jar"));
        Job job = JetEngine.getJob(getHazelcastInstance(), name, dag, jobConfig);

        execute(job);
    }

    @Test
    public void test_Class_Distribution() throws Exception {
        getFactory().newInstances(new Config(), 3);

        DAG dag = new DAG();
        dag.addVertex(createVertex("create and print person", PrintPersonVertex.class));
        String name = generateRandomString(10);
        JobConfig jobConfig = new JobConfig(name);
        URL classUrl = this.getClass().getResource("/cp1/");
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{classUrl}, null);
        Class<?> appearance = urlClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");
        jobConfig.addClass(appearance);
        Job job = JetEngine.getJob(getHazelcastInstance(), name, dag, jobConfig);

        execute(job);

    }

    @Test
    public void test_Resource_Distribution() throws Exception {
        getFactory().newInstances(new Config(), 3);

        DAG dag = new DAG();
        dag.addVertex(createVertex("apachev1", ApacheV1.class));

        String name = generateRandomString(10);
        JobConfig jobConfig = new JobConfig(name);
        jobConfig.addResource(new URL("http://www.apache.org/licenses/LICENSE-1.1.txt"), "apachev1");
        Job job = JetEngine.getJob(getHazelcastInstance(), name, dag, jobConfig);

        execute(job);
    }

    @Test
    public void test_Jar_Isolation() throws Exception {
        getFactory().newInstances(new Config(), 3);

        DAG dag1 = new DAG();
        dag1.addVertex(createVertex("create and print person", PrintPersonVertex.class));
        String name1 = generateRandomString(10);
        JobConfig jobConfig1 = new JobConfig(name1);
        jobConfig1.addJar(this.getClass().getResource("/sample-pojo-1.0-person.jar"));

        Job job1 = JetEngine.getJob(getHazelcastInstance(), name1, dag1, jobConfig1);

        DAG dag2 = new DAG();
        dag2.addVertex(createVertex("create and print car", PrintCarVertex.class));
        String name2 = generateRandomString(10);
        JobConfig jobConfig2 = new JobConfig(name2);
        jobConfig2.addJar(this.getClass().getResource("/sample-pojo-1.0-car.jar"));

        Job job2 = JetEngine.getJob(getHazelcastInstance(), name2, dag2, jobConfig2);

        Future f1 = job1.execute();
        Future f2 = job2.execute();

        assertCompletesEventually(f1);
        assertCompletesEventually(f2);

        f1.get();
        f2.get();
    }

    @Test
    public void test_Class_Isolation() throws Exception {
        getFactory().newInstances(new Config(), 3);

        DAG dag1 = new DAG();
        dag1.addVertex(createVertex("create and print person", PrintPersonVertex.class));
        URL classUrl1 = this.getClass().getResource("/cp1/");
        URLClassLoader urlClassLoader1 = new URLClassLoader(new URL[]{classUrl1}, null);
        Class<?> appearance = urlClassLoader1.loadClass("com.sample.pojo.person.Person$Appereance");
        String name1 = generateRandomString(10);
        JobConfig jobConfig1 = new JobConfig(name1);
        jobConfig1.addClass(appearance);

        Job job1 = JetEngine.getJob(getHazelcastInstance(), name1, dag1, jobConfig1);


        DAG dag2 = new DAG();
        dag2.addVertex(createVertex("create and print car", PrintCarVertex.class));
        URL classUrl2 = this.getClass().getResource("/cp2/");
        URLClassLoader urlClassLoader2 = new URLClassLoader(new URL[]{classUrl2}, null);
        Class<?> car = urlClassLoader2.loadClass("com.sample.pojo.car.Car");
        String name2 = generateRandomString(10);
        JobConfig jobConfig2 = new JobConfig(name2);
        jobConfig2.addClass(car);

        Job job2 = JetEngine.getJob(getHazelcastInstance(), name2, dag2, jobConfig2);

        Future f1 = job1.execute();
        Future f2 = job2.execute();

        assertCompletesEventually(f1);
        assertCompletesEventually(f2);
        f1.get();
        f2.get();
    }

    @Test
    public void test_Resource_Isolation() throws Exception {
        getFactory().newInstances(new Config(), 3);

        DAG dag1 = new DAG();
        dag1.addVertex(createVertex("apachev1", ApacheV1.class));
        String name1 = generateRandomString(10);
        JobConfig jobConfig1 = new JobConfig(name1);
        jobConfig1.addResource(new URL("http://www.apache.org/licenses/LICENSE-1.1.txt"), "apachev1");

        Job job1 = JetEngine.getJob(getHazelcastInstance(), name1, dag1, jobConfig1);


        DAG dag2 = new DAG();
        dag2.addVertex(createVertex("apachev2", ApacheV2.class));
        String name2 = generateRandomString(10);
        JobConfig jobConfig2 = new JobConfig(name2);
        jobConfig2.addResource(new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"), "apachev2");

        Job job2 = JetEngine.getJob(getHazelcastInstance(), name2, dag2, jobConfig2);

        Future f1 = job1.execute();
        Future f2 = job2.execute();

        assertCompletesEventually(f1);
        assertCompletesEventually(f2);

        f1.get();
        f2.get();

    }

}
