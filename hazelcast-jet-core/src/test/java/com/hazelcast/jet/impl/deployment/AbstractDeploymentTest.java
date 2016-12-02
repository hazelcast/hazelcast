package com.hazelcast.jet.impl.deployment;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetEngine;
import com.hazelcast.jet.JetEngineConfig;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Vertex;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.hazelcast.jet.TestUtil.executeAndPeel;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractDeploymentTest extends HazelcastTestSupport {

    abstract TestHazelcastInstanceFactory getFactory();

    abstract HazelcastInstance getHazelcastInstance();

    @Test
    public void test_Jar_Distribution() throws Throwable {
        getFactory().newInstances(new Config(), 3);

        DAG dag = new DAG();
        dag.addVertex(new Vertex("create and print person", LoadPersonIsolated::new));

        String name = generateRandomString(10);
        JetEngineConfig jetEngineConfig = new JetEngineConfig();
        jetEngineConfig.addJar(this.getClass().getResource("/sample-pojo-1.0-person.jar"));
        JetEngine jetEngine = JetEngine.get(getHazelcastInstance(), name, jetEngineConfig);
        executeAndPeel(jetEngine.newJob(dag));
    }

    @Test
    public void test_Class_Distribution() throws Throwable {
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

        executeAndPeel(jetEngine.newJob(dag));
    }

    @Test
    public void test_Resource_Distribution() throws Throwable {
        getFactory().newInstances(new Config(), 3);

        DAG dag = new DAG();
        dag.addVertex(new Vertex("apachev1", ApacheV1Isolated::new));

        String name = generateRandomString(10);
        JetEngineConfig jetEngineConfig = new JetEngineConfig();
        jetEngineConfig.addResource(new URL("http://www.apache.org/licenses/LICENSE-1.1.txt"), "apachev1");
        JetEngine jetEngine = JetEngine.get(getHazelcastInstance(), name, jetEngineConfig);

        executeAndPeel(jetEngine.newJob(dag));
    }

    @Test
    public void test_Jar_Isolation_Between_Engines() throws Throwable {
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

        executeAndPeel(job1);
        executeAndPeel(job2);
    }

    @Test
    public void test_Class_Isolation_Between_Engines() throws Throwable {
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

        executeAndPeel(job1);
        executeAndPeel(job2);
    }

    @Test
    public void test_Resource_Isolation_Between_Engines() throws Throwable {
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

        executeAndPeel(job1);
        executeAndPeel(job2);
    }

    @Test
    public void test_Jar_Sharing_Between_Jobs() throws Throwable {
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

        executeAndPeel(job1);
        executeAndPeel(job2);
    }

    @Test
    public void test_Class_Sharing_Between_Jobs() throws Throwable {
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

        executeAndPeel(job1);
        executeAndPeel(job2);
    }

    @Test
    public void test_Resource_Sharing_Between_Jobs() throws Throwable {
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

        executeAndPeel(job1);
        executeAndPeel(job2);
    }


    private static class ApacheV1andV2 extends AbstractProcessor {

        @Override
        public boolean complete() {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            URL resource = contextClassLoader.getResource("apachev1");
            assertNotNull(resource);
            BufferedReader reader = null;
            try {
                reader = Files.newBufferedReader(Paths.get(resource.toURI()));
                String firstLine = reader.readLine();
                String secondLine = reader.readLine();
                assertTrue(secondLine.contains("Version 1.1"));
                assertNotNull(contextClassLoader.getResourceAsStream("apachev2"));

            } catch (IOException | URISyntaxException e) {
                fail();
            }
            return true;

        }
    }

    private static class ApacheV1Isolated extends AbstractProcessor {

        @Override
        public boolean complete() {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            URL resource = contextClassLoader.getResource("apachev1");
            assertNotNull(resource);
            BufferedReader reader = null;
            try {
                reader = Files.newBufferedReader(Paths.get(resource.toURI()));
                String firstLine = reader.readLine();
                String secondLine = reader.readLine();
                assertTrue(secondLine.contains("Version 1.1"));
                assertNull(contextClassLoader.getResourceAsStream("apachev2"));

            } catch (IOException | URISyntaxException e) {
                fail();
            }
            return true;

        }
    }

    private static class LoadCarIsolated extends AbstractProcessor {

        @Override
        public boolean complete() {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                contextClassLoader.loadClass("com.sample.pojo.car.Car");
            } catch (ClassNotFoundException e) {
                fail();
            }
            try {
                contextClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");
                fail();
            } catch (ClassNotFoundException ignored) {
            }
            return true;
        }
    }

    private static class ApacheV2 extends AbstractProcessor {

        @Override
        public boolean complete() {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            URL resource = contextClassLoader.getResource("apachev2");
            BufferedReader reader = null;
            try {
                reader = Files.newBufferedReader(Paths.get(resource.toURI()));
                String firstLine = reader.readLine();
                String secondLine = reader.readLine();
                assertTrue(secondLine.contains("Apache"));
                assertNull(contextClassLoader.getResourceAsStream("apachev1"));
            } catch (IOException | URISyntaxException e) {
                fail();
            }
            return true;
        }
    }

    private static class LoadPersonAndCar extends AbstractProcessor {

        @Override
        public boolean complete() {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                contextClassLoader.loadClass("com.sample.pojo.car.Car");
            } catch (ClassNotFoundException e) {
                fail();
            }
            try {
                contextClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");
            } catch (ClassNotFoundException ignored) {
                fail();
            }
            return true;
        }
    }

    private static class LoadPersonIsolated extends AbstractProcessor {

        @Override
        public boolean complete() {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                contextClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");
            } catch (ClassNotFoundException e) {
                fail(e.getMessage());
            }
            try {
                contextClassLoader.loadClass("com.sample.pojo.car.Car");
                fail();
            } catch (ClassNotFoundException ignored) {
            }
            return true;
        }
    }
}
