package com.hazelcast.jet.impl.job.deployment;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetEngine;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.impl.job.deployment.processors.ApacheV1;
import com.hazelcast.jet.impl.job.deployment.processors.ApacheV2;
import com.hazelcast.jet.impl.job.deployment.processors.PrintCarVertex;
import com.hazelcast.jet.impl.job.deployment.processors.PrintPersonVertex;
import com.hazelcast.jet.job.Job;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;
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
        URL gzipResource = this.getClass().getResource("/Person$Appereance.class.gz");
        File classFile = createClassFileFromGzip(gzipResource, "Person$Appereance.class");
        jobConfig.addClass(classFile.toURI().toURL(), "com.sample.pojo.person.Person$Appereance");
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
        URL gzipResource1 = this.getClass().getResource("/Person$Appereance.class.gz");
        File classFile1 = createClassFileFromGzip(gzipResource1, "Person$Appereance.class");
        String name1 = generateRandomString(10);
        JobConfig jobConfig1 = new JobConfig(name1);
        jobConfig1.addClass(classFile1.toURI().toURL(), "com.sample.pojo.person.Person$Appereance");

        Job job1 = JetEngine.getJob(getHazelcastInstance(), name1, dag1, jobConfig1);


        DAG dag2 = new DAG();
        dag2.addVertex(createVertex("create and print car", PrintCarVertex.class));
        URL gzipResource2 = this.getClass().getResource("/Car.class.gz");
        File classFile2 = createClassFileFromGzip(gzipResource2, "Car.class");
        String name2 = generateRandomString(10);
        JobConfig jobConfig2 = new JobConfig(name2);
        jobConfig2.addClass(classFile2.toURI().toURL(), "com.sample.pojo.car.Car");

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

    private File createClassFileFromGzip(URL gzipResource, String className) throws IOException {
        GZIPInputStream inputStream = new GZIPInputStream(gzipResource.openStream());
        String tempDir = System.getProperty("java.io.tmpdir");
        File classFile = new File(tempDir, className);
        classFile.deleteOnExit();
        FileOutputStream outputStream = new FileOutputStream(classFile);
        byte[] buffer = new byte[1024];
        int len;
        while ((len = inputStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, len);
        }
        outputStream.flush();
        outputStream.close();
        inputStream.close();
        return classFile;
    }
}
