package com.hazelcast.jet2.impl.deployment;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet2.DAG;
import com.hazelcast.jet2.JetEngine;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.jet2.Job;
import com.hazelcast.jet2.Vertex;
import com.hazelcast.jet2.impl.deployment.processors.ApacheV1Isolated;
import com.hazelcast.jet2.impl.deployment.processors.ApacheV1andV2;
import com.hazelcast.jet2.impl.deployment.processors.ApacheV2;
import com.hazelcast.jet2.impl.deployment.processors.LoadCarIsolated;
import com.hazelcast.jet2.impl.deployment.processors.LoadPersonAndCar;
import com.hazelcast.jet2.impl.deployment.processors.LoadPersonIsolated;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import java.net.URL;
import java.net.URLClassLoader;
import org.junit.Test;

public abstract class AbstractDeploymentTest extends HazelcastTestSupport {

    abstract TestHazelcastInstanceFactory getFactory();

    abstract HazelcastInstance getHazelcastInstance();

    @Test
    public void test_Jar_Distribution() throws Exception {
        getFactory().newInstances(new Config(), 3);

        DAG dag = new DAG();
        dag.addVertex(new Vertex("create and print person", LoadPersonIsolated::new));

        String name = generateRandomString(10);
        JetEngineConfig jetEngineConfig = new JetEngineConfig();
        jetEngineConfig.addJar(this.getClass().getResource("/sample-pojo-1.0-person.jar"));
        JetEngine jetEngine = JetEngine.get(getHazelcastInstance(), name, jetEngineConfig);
        Job job = jetEngine.newJob(dag);
        job.execute();
    }

    @Test
    public void test_Class_Distribution() throws Exception {
        getFactory().newInstances(new Config(), 3);

        DAG dag = new DAG();
        dag.addVertex(new Vertex("create and print person", LoadPersonIsolated::new));
        String name = generateRandomString(10);
        JetEngineConfig jetEngineConfig = new JetEngineConfig();
        URL classUrl = this.getClass().getResource("/cp1/");
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{classUrl}, null);
        Class<?> appearance = urlClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");
        jetEngineConfig.addClass(appearance);
        JetEngine jetEngine = JetEngine.get(getHazelcastInstance(), name, jetEngineConfig);

        Job job = jetEngine.newJob(dag);
        job.execute();
    }

    @Test
    public void test_Resource_Distribution() throws Exception {
        getFactory().newInstances(new Config(), 3);

        DAG dag = new DAG();
        dag.addVertex(new Vertex("apachev1", ApacheV1Isolated::new));

        String name = generateRandomString(10);
        JetEngineConfig jetEngineConfig = new JetEngineConfig();
        jetEngineConfig.addResource(new URL("http://www.apache.org/licenses/LICENSE-1.1.txt"), "apachev1");
        JetEngine jetEngine = JetEngine.get(getHazelcastInstance(), name, jetEngineConfig);
        Job job = jetEngine.newJob(dag);
        job.execute();
    }

    @Test
    public void test_Jar_Isolation_Between_Engines() throws Exception {
        getFactory().newInstances(new Config(), 3);

        DAG dag1 = new DAG();
        dag1.addVertex(new Vertex("create and print person", LoadPersonIsolated::new));
        String name1 = generateRandomString(10);
        JetEngineConfig jetEngineConfig1 = new JetEngineConfig();
        jetEngineConfig1.addJar(this.getClass().getResource("/sample-pojo-1.0-person.jar"));

        JetEngine jetEngine1 = JetEngine.get(getHazelcastInstance(), name1, jetEngineConfig1);
        Job job1 = jetEngine1.newJob(dag1);

        DAG dag2 = new DAG();
        dag2.addVertex(new Vertex("create and print car", LoadCarIsolated::new));
        String name2 = generateRandomString(10);
        JetEngineConfig jetEngineConfig2 = new JetEngineConfig();
        jetEngineConfig2.addJar(this.getClass().getResource("/sample-pojo-1.0-car.jar"));

        JetEngine jetEngine2 = JetEngine.get(getHazelcastInstance(), name2, jetEngineConfig2);
        Job job2 = jetEngine2.newJob(dag2);

        job1.execute();
        job2.execute();
    }

    @Test
    public void test_Class_Isolation_Between_Engines() throws Exception {
        getFactory().newInstances(new Config(), 3);

        DAG dag1 = new DAG();
        dag1.addVertex(new Vertex("create and print person", LoadPersonIsolated::new));
        URL classUrl1 = this.getClass().getResource("/cp1/");
        URLClassLoader urlClassLoader1 = new URLClassLoader(new URL[]{classUrl1}, null);
        Class<?> appearance = urlClassLoader1.loadClass("com.sample.pojo.person.Person$Appereance");
        String name1 = generateRandomString(10);
        JetEngineConfig jetEngineConfig1 = new JetEngineConfig();
        jetEngineConfig1.addClass(appearance);

        JetEngine jetEngine1 = JetEngine.get(getHazelcastInstance(), name1, jetEngineConfig1);
        Job job1 = jetEngine1.newJob(dag1);


        DAG dag2 = new DAG();
        dag2.addVertex(new Vertex("create and print car", LoadCarIsolated::new));
        URL classUrl2 = this.getClass().getResource("/cp2/");
        URLClassLoader urlClassLoader2 = new URLClassLoader(new URL[]{classUrl2}, null);
        Class<?> car = urlClassLoader2.loadClass("com.sample.pojo.car.Car");
        String name2 = generateRandomString(10);
        JetEngineConfig jetEngineConfig2 = new JetEngineConfig();
        jetEngineConfig2.addClass(car);

        JetEngine jetEngine2 = JetEngine.get(getHazelcastInstance(), name2, jetEngineConfig2);
        Job job2 = jetEngine2.newJob(dag2);

        job1.execute();
        job2.execute();
    }

    @Test
    public void test_Resource_Isolation_Between_Engines() throws Exception {
        getFactory().newInstances(new Config(), 3);

        DAG dag1 = new DAG();
        dag1.addVertex(new Vertex("apachev1", ApacheV1Isolated::new));
        String name1 = generateRandomString(10);
        JetEngineConfig jetEngineConfig1 = new JetEngineConfig();
        jetEngineConfig1.addResource(new URL("http://www.apache.org/licenses/LICENSE-1.1.txt"), "apachev1");

        JetEngine jetEngine = JetEngine.get(getHazelcastInstance(), name1, jetEngineConfig1);
        Job job1 = jetEngine.newJob(dag1);


        DAG dag2 = new DAG();
        dag2.addVertex(new Vertex("apachev2", ApacheV2::new));
        String name2 = generateRandomString(10);
        JetEngineConfig jetEngineConfig2 = new JetEngineConfig();
        jetEngineConfig2.addResource(new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"), "apachev2");

        JetEngine jetEngine2 = JetEngine.get(getHazelcastInstance(), name2, jetEngineConfig2);
        Job job2 = jetEngine2.newJob(dag2);

        job1.execute();
        job2.execute();
    }

    @Test
    public void test_Jar_Sharing_Between_Jobs() throws Exception {
        getFactory().newInstances(new Config(), 3);

        DAG dag1 = new DAG();
        dag1.addVertex(new Vertex("load person and car", LoadPersonAndCar::new));
        String name1 = generateRandomString(10);
        JetEngineConfig jetEngineConfig = new JetEngineConfig();
        jetEngineConfig.addJar(this.getClass().getResource("/sample-pojo-1.0-person.jar"));
        jetEngineConfig.addJar(this.getClass().getResource("/sample-pojo-1.0-car.jar"));

        JetEngine jetEngine = JetEngine.get(getHazelcastInstance(), name1, jetEngineConfig);
        Job job1 = jetEngine.newJob(dag1);

        DAG dag2 = new DAG();
        dag2.addVertex(new Vertex("load person and car", LoadPersonAndCar::new));
        Job job2 = jetEngine.newJob(dag2);

        job1.execute();
        job2.execute();
    }

    @Test
    public void test_Class_Sharing_Between_Jobs() throws Exception {
        getFactory().newInstances(new Config(), 3);


        URL classUrl1 = this.getClass().getResource("/cp1/");
        URLClassLoader urlClassLoader1 = new URLClassLoader(new URL[]{classUrl1}, null);
        Class<?> appearance = urlClassLoader1.loadClass("com.sample.pojo.person.Person$Appereance");

        URL classUrl2 = this.getClass().getResource("/cp2/");
        URLClassLoader urlClassLoader2 = new URLClassLoader(new URL[]{classUrl2}, null);
        Class<?> car = urlClassLoader2.loadClass("com.sample.pojo.car.Car");

        JetEngineConfig jetEngineConfig1 = new JetEngineConfig();
        jetEngineConfig1.addClass(appearance);
        jetEngineConfig1.addClass(car);

        String name1 = generateRandomString(10);
        JetEngine jetEngine1 = JetEngine.get(getHazelcastInstance(), name1, jetEngineConfig1);

        DAG dag1 = new DAG();
        dag1.addVertex(new Vertex("load person and car", LoadPersonAndCar::new));
        Job job1 = jetEngine1.newJob(dag1);


        DAG dag2 = new DAG();
        dag2.addVertex(new Vertex("load person and car", LoadPersonAndCar::new));
        Job job2 = jetEngine1.newJob(dag2);

        job1.execute();
        job2.execute();
    }

    @Test
    public void test_Resource_Sharing_Between_Jobs() throws Exception {
        getFactory().newInstances(new Config(), 3);

        DAG dag1 = new DAG();
        dag1.addVertex(new Vertex("apachev1andv2", ApacheV1andV2::new));
        String name1 = generateRandomString(10);
        JetEngineConfig jetEngineConfig1 = new JetEngineConfig();
        jetEngineConfig1.addResource(new URL("http://www.apache.org/licenses/LICENSE-1.1.txt"), "apachev1");
        jetEngineConfig1.addResource(new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"), "apachev2");

        JetEngine jetEngine = JetEngine.get(getHazelcastInstance(), name1, jetEngineConfig1);
        Job job1 = jetEngine.newJob(dag1);


        DAG dag2 = new DAG();
        dag2.addVertex(new Vertex("apachev1andv2", ApacheV1andV2::new));
        Job job2 = jetEngine.newJob(dag2);

        job1.execute();
        job2.execute();
    }


}
