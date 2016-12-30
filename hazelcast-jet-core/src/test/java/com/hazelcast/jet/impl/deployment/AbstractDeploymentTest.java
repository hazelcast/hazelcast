//package com.hazelcast.jet.impl.deployment;
//
//import com.hazelcast.config.Config;
//import com.hazelcast.core.HazelcastInstance;
//import com.hazelcast.jet.AbstractProcessor;
//import com.hazelcast.jet.DAG;
//import com.hazelcast.jet.JetConfig;
//import com.hazelcast.jet.Job;
//import com.hazelcast.jet.Vertex;
//import com.hazelcast.test.HazelcastTestSupport;
//import com.hazelcast.test.TestHazelcastInstanceFactory;
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.net.URISyntaxException;
//import java.net.URL;
//import java.net.URLClassLoader;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//
//import org.junit.Ignore;
//import org.junit.Test;
//
//import static com.hazelcast.jet.TestUtil.executeAndPeel;
//import static org.junit.Assert.assertNotNull;
//import static org.junit.Assert.assertNull;
//import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.fail;
//
//@Ignore
//public abstract class AbstractDeploymentTest extends HazelcastTestSupport {
//
//    abstract TestHazelcastInstanceFactory getFactory();
//
//    abstract HazelcastInstance getHazelcastInstance();
//
//    @Test
//    public void test_Jar_Distribution() throws Throwable {
//        createCluster(2);
//
//        DAG dag = new DAG();
//        dag.addVertex(new Vertex("create and print person", LoadPersonIsolated::new));
//
//        String name = generateRandomString(10);
//        JetConfig jetConfig = new JetConfig();
////        jetConfig.addJar(this.getClass().getResource("/sample-pojo-1.0-person.jar"));
//        JetEngine jetEngine = JetEngine.get(getHazelcastInstance(), name, jetConfig);
//        executeAndPeel(jetEngine.newJob(dag));
//    }
//
//    @Test
//    public void test_Class_Distribution() throws Throwable {
//        createCluster(2);
//
//        DAG dag = new DAG();
//        dag.addVertex(new Vertex("create and print person", LoadPersonIsolated::new));
//        String name = generateRandomString(10);
//        JetConfig jetConfig = new JetConfig();
//        URL classUrl = this.getClass().getResource("/cp1/");
//        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{classUrl}, null);
//        Class<?> appearance = urlClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");
////        jetConfig.addClass(appearance);
//        JetEngine jetEngine = JetEngine.get(getHazelcastInstance(), name, jetConfig);
//
//        executeAndPeel(jetEngine.newJob(dag));
//    }
//
//    @Test
//    public void test_Resource_Distribution() throws Throwable {
//        createCluster(2);
//
//        DAG dag = new DAG();
//        dag.addVertex(new Vertex("apachev1", ApacheV1Isolated::new));
//
//        String name = generateRandomString(10);
//        JetConfig jetConfig = new JetConfig();
////        jetConfig.addResource(new URL("http://www.apache.org/licenses/LICENSE-1.1.txt"), "apachev1");
//        JetEngine jetEngine = JetEngine.get(getHazelcastInstance(), name, jetConfig);
//
//        executeAndPeel(jetEngine.newJob(dag));
//    }
//
//    @Test
//    public void test_Jar_Isolation_Between_Engines() throws Throwable {
//        createCluster(2);
//
//        DAG dag1 = new DAG();
//        dag1.addVertex(new Vertex("create and print person", LoadPersonIsolated::new));
//        String name1 = generateRandomString(10);
//        JetConfig jetConfig1 = new JetConfig();
////        jetConfig1.addJar(this.getClass().getResource("/sample-pojo-1.0-person.jar"));
//
//        JetEngine jetEngine1 = JetEngine.get(getHazelcastInstance(), name1, jetConfig1);
//        Job job1 = jetEngine1.newJob(dag1);
//
//        DAG dag2 = new DAG();
//        dag2.addVertex(new Vertex("create and print car", LoadCarIsolated::new));
//        String name2 = generateRandomString(10);
//        JetConfig jetConfig2 = new JetConfig();
////        jetConfig2.addJar(this.getClass().getResource("/sample-pojo-1.0-car.jar"));
//
//        JetEngine jetEngine2 = JetEngine.get(getHazelcastInstance(), name2, jetConfig2);
//        Job job2 = jetEngine2.newJob(dag2);
//
//        executeAndPeel(job1);
//        executeAndPeel(job2);
//    }
//
//    @Test
//    public void test_Class_Isolation_Between_Engines() throws Throwable {
//        createCluster(2);
//
//        DAG dag1 = new DAG();
//        dag1.addVertex(new Vertex("create and print person", LoadPersonIsolated::new));
//        URL classUrl1 = this.getClass().getResource("/cp1/");
//        URLClassLoader urlClassLoader1 = new URLClassLoader(new URL[]{classUrl1}, null);
//        Class<?> appearance = urlClassLoader1.loadClass("com.sample.pojo.person.Person$Appereance");
//        String name1 = generateRandomString(10);
//        JetConfig jetConfig1 = new JetConfig();
////        jetConfig1.addClass(appearance);
//
//        JetEngine jetEngine1 = JetEngine.get(getHazelcastInstance(), name1, jetConfig1);
//        Job job1 = jetEngine1.newJob(dag1);
//
//
//        DAG dag2 = new DAG();
//        dag2.addVertex(new Vertex("create and print car", LoadCarIsolated::new));
//        URL classUrl2 = this.getClass().getResource("/cp2/");
//        URLClassLoader urlClassLoader2 = new URLClassLoader(new URL[]{classUrl2}, null);
//        Class<?> car = urlClassLoader2.loadClass("com.sample.pojo.car.Car");
//        String name2 = generateRandomString(10);
//        JetConfig jetConfig2 = new JetConfig();
////        jetConfig2.addClass(car);
//
//        JetEngine jetEngine2 = JetEngine.get(getHazelcastInstance(), name2, jetConfig2);
//        Job job2 = jetEngine2.newJob(dag2);
//
//        executeAndPeel(job1);
//        executeAndPeel(job2);
//    }
//
//    @Test
//    public void test_Resource_Isolation_Between_Engines() throws Throwable {
//        createCluster(2);
//
//        DAG dag1 = new DAG();
//        dag1.addVertex(new Vertex("apachev1", ApacheV1Isolated::new));
//        String name1 = generateRandomString(10);
//        JetConfig jetConfig1 = new JetConfig();
////        jetConfig1.addResource(new URL("http://www.apache.org/licenses/LICENSE-1.1.txt"), "apachev1");
//
//        JetEngine jetEngine = JetEngine.get(getHazelcastInstance(), name1, jetConfig1);
//        Job job1 = jetEngine.newJob(dag1);
//
//
//        DAG dag2 = new DAG();
//        dag2.addVertex(new Vertex("apachev2", ApacheV2::new));
//        String name2 = generateRandomString(10);
//        JetConfig jetConfig2 = new JetConfig();
////        jetConfig2.addResource(new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"), "apachev2");
//
//        JetEngine jetEngine2 = JetEngine.get(getHazelcastInstance(), name2, jetConfig2);
//        Job job2 = jetEngine2.newJob(dag2);
//
//        executeAndPeel(job1);
//        executeAndPeel(job2);
//    }
//
//    @Test
//    public void test_Jar_Sharing_Between_Jobs() throws Throwable {
//        createCluster(2);
//
//        DAG dag1 = new DAG();
//        dag1.addVertex(new Vertex("load person and car", LoadPersonAndCar::new));
//        String name1 = generateRandomString(10);
//        JetConfig jetConfig = new JetConfig();
////        jetConfig.addJar(this.getClass().getResource("/sample-pojo-1.0-person.jar"));
////        jetConfig.addJar(this.getClass().getResource("/sample-pojo-1.0-car.jar"));
//
//        JetEngine jetEngine = JetEngine.get(getHazelcastInstance(), name1, jetConfig);
//        Job job1 = jetEngine.newJob(dag1);
//
//        DAG dag2 = new DAG();
//        dag2.addVertex(new Vertex("load person and car", LoadPersonAndCar::new));
//        Job job2 = jetEngine.newJob(dag2);
//
//        executeAndPeel(job1);
//        executeAndPeel(job2);
//    }
//
//    @Test
//    public void test_Class_Sharing_Between_Jobs() throws Throwable {
//        createCluster(2);
//
//        URL classUrl1 = this.getClass().getResource("/cp1/");
//        URLClassLoader urlClassLoader1 = new URLClassLoader(new URL[]{classUrl1}, null);
//        Class<?> appearance = urlClassLoader1.loadClass("com.sample.pojo.person.Person$Appereance");
//
//        URL classUrl2 = this.getClass().getResource("/cp2/");
//        URLClassLoader urlClassLoader2 = new URLClassLoader(new URL[]{classUrl2}, null);
//        Class<?> car = urlClassLoader2.loadClass("com.sample.pojo.car.Car");
//
//        JetConfig jetConfig1 = new JetConfig();
////        jetConfig1.addClass(appearance);
////        jetConfig1.addClass(car);
//
//        String name1 = generateRandomString(10);
//        JetEngine jetEngine1 = JetEngine.get(getHazelcastInstance(), name1, jetConfig1);
//
//        DAG dag1 = new DAG();
//        dag1.addVertex(new Vertex("load person and car", LoadPersonAndCar::new));
//        Job job1 = jetEngine1.newJob(dag1);
//
//
//        DAG dag2 = new DAG();
//        dag2.addVertex(new Vertex("load person and car", LoadPersonAndCar::new));
//        Job job2 = jetEngine1.newJob(dag2);
//
//        executeAndPeel(job1);
//        executeAndPeel(job2);
//    }
//
//    @Test
//    public void test_Resource_Sharing_Between_Jobs() throws Throwable {
//        createCluster(2);
//
//        DAG dag1 = new DAG();
//        dag1.addVertex(new Vertex("apachev1andv2", ApacheV1andV2::new));
//        String name1 = generateRandomString(10);
//        JetConfig jetConfig1 = new JetConfig();
////        jetConfig1.addResource(new URL("http://www.apache.org/licenses/LICENSE-1.1.txt"), "apachev1");
////        jetConfig1.addResource(new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"), "apachev2");
//
//        JetEngine jetEngine = JetEngine.get(getHazelcastInstance(), name1, jetConfig1);
//        Job job1 = jetEngine.newJob(dag1);
//
//
//        DAG dag2 = new DAG();
//        dag2.addVertex(new Vertex("apachev1andv2", ApacheV1andV2::new));
//        Job job2 = jetEngine.newJob(dag2);
//
//        executeAndPeel(job1);
//        executeAndPeel(job2);
//    }
//
//    private void createCluster(int nodeCount) {
//        HazelcastInstance[] instances = getFactory().newInstances(new Config(), nodeCount);
//        for (HazelcastInstance instance : instances) {
//            assertClusterSizeEventually(nodeCount, instance);
//        }
//    }
//
//
//    private static class ApacheV1andV2 extends AbstractProcessor {
//
//        @Override
//        public boolean complete() {
//            ClassLoader cl = getClass().getClassLoader();
//            URL resource = cl.getResource("apachev1");
//            assertNotNull(resource);
//            BufferedReader reader = null;
//            try {
//                reader = Files.newBufferedReader(Paths.get(resource.toURI()));
//                String firstLine = reader.readLine();
//                String secondLine = reader.readLine();
//                assertTrue(secondLine.contains("Version 1.1"));
//                assertNotNull(cl.getResourceAsStream("apachev2"));
//
//            } catch (IOException | URISyntaxException e) {
//                fail();
//            }
//            return true;
//
//        }
//    }
//
//    private static class ApacheV1Isolated extends AbstractProcessor {
//
//        @Override
//        public boolean complete() {
//            ClassLoader cl = getClass().getClassLoader();
//            URL resource = cl.getResource("apachev1");
//            assertNotNull(resource);
//            BufferedReader reader = null;
//            try {
//                reader = Files.newBufferedReader(Paths.get(resource.toURI()));
//                String firstLine = reader.readLine();
//                String secondLine = reader.readLine();
//                assertTrue(secondLine.contains("Version 1.1"));
//                assertNull(cl.getResourceAsStream("apachev2"));
//
//            } catch (IOException | URISyntaxException e) {
//                fail();
//            }
//            return true;
//
//        }
//    }
//
//    private static class LoadCarIsolated extends AbstractProcessor {
//
//        @Override
//        public boolean complete() {
//            ClassLoader cl = getClass().getClassLoader();
//            try {
//                cl.loadClass("com.sample.pojo.car.Car");
//            } catch (ClassNotFoundException e) {
//                fail();
//            }
//            try {
//                cl.loadClass("com.sample.pojo.person.Person$Appereance");
//                fail();
//            } catch (ClassNotFoundException ignored) {
//            }
//            return true;
//        }
//    }
//
//    private static class ApacheV2 extends AbstractProcessor {
//
//        @Override
//        public boolean complete() {
//            ClassLoader cl = getClass().getClassLoader();
//            URL resource = cl.getResource("apachev2");
//            BufferedReader reader = null;
//            try {
//                reader = Files.newBufferedReader(Paths.get(resource.toURI()));
//                String firstLine = reader.readLine();
//                String secondLine = reader.readLine();
//                assertTrue(secondLine.contains("Apache"));
//                assertNull(cl.getResourceAsStream("apachev1"));
//            } catch (IOException | URISyntaxException e) {
//                fail();
//            }
//            return true;
//        }
//    }
//
//    private static class LoadPersonAndCar extends AbstractProcessor {
//
//        @Override
//        public boolean complete() {
//            ClassLoader cl = getClass().getClassLoader();
//            try {
//                cl.loadClass("com.sample.pojo.car.Car");
//            } catch (ClassNotFoundException e) {
//                fail();
//            }
//            try {
//                cl.loadClass("com.sample.pojo.person.Person$Appereance");
//            } catch (ClassNotFoundException ignored) {
//                fail();
//            }
//            return true;
//        }
//    }
//
//    private static class LoadPersonIsolated extends AbstractProcessor {
//
//        @Override
//        public boolean complete() {
//            ClassLoader cl = getClass().getClassLoader();
//            try {
//                cl.loadClass("com.sample.pojo.person.Person$Appereance");
//            } catch (ClassNotFoundException e) {
//                fail(e.getMessage());
//            }
//            try {
//                cl.loadClass("com.sample.pojo.car.Car");
//                fail();
//            } catch (ClassNotFoundException ignored) {
//            }
//            return true;
//        }
//    }
//}
