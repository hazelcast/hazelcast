package com.hazelcast.jet.deployment;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetEngine;
import com.hazelcast.jet.JetTestSupport;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.deployment.processors.Apache;
import com.hazelcast.jet.deployment.processors.GPL;
import com.hazelcast.jet.deployment.processors.PrintCarVertex;
import com.hazelcast.jet.deployment.processors.PrintPersonVertex;
import com.hazelcast.jet.impl.job.deployment.ResourceType;
import com.hazelcast.jet.job.Job;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ClientDeploymentTest extends JetTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void test_Jar_Distribution() throws Exception {
        factory.newInstances(new Config(), 3);
        final Job job = JetEngine.getJob(factory.newHazelcastClient(), generateRandomString(10));

        DAG dag = new DAG();
        dag.addVertex(createVertex("create and print person", PrintPersonVertex.class));
        job.addResource(this.getClass().getResource("/sample-pojo-1.0-person.jar"));

        job.submit(dag);
        execute(job);
    }


    @Test
    public void test_Class_Distribution() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();
        final Job job = JetEngine.getJob(instances[0], generateRandomString(10));

        DAG dag = new DAG();
        dag.addVertex(createVertex("create and print person", PrintPersonVertex.class));
        URL gzipResource = this.getClass().getResource("/Person$Appereance.class.gz");
        File classFile = createClassFileFromGzip(gzipResource, "Person$Appereance.class");

        job.addResource(classFile.toURI().toURL().openStream(), "com.sample.pojo.person.Person$Appereance", ResourceType.CLASS);

        job.submit(dag);
        execute(job);

    }


    @Test
    public void test_Resource_Distribution() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances();
        final Job job = JetEngine.getJob(instances[0], generateRandomString(10));
        DAG dag = new DAG();
        dag.addVertex(createVertex("gpl", GPL.class));
        job.addResource(new URL("https://www.gnu.org/licenses/gpl-3.0.txt").openStream(), "gpl", ResourceType.DATA);
        job.submit(dag);
        execute(job);
    }


    @Test
    public void test_Jar_Isolation() throws Exception {
        factory.newInstances(new Config(), 3);
        HazelcastInstance client = factory.newHazelcastClient();
        final Job job1 = JetEngine.getJob(client, generateRandomString(10));
        final Job job2 = JetEngine.getJob(client, generateRandomString(10));

        DAG dag1 = new DAG();
        dag1.addVertex(createVertex("create and print person", PrintPersonVertex.class));
        job1.addResource(this.getClass().getResource("/sample-pojo-1.0-person.jar"));
        job1.submit(dag1);

        DAG dag2 = new DAG();
        dag2.addVertex(createVertex("create and print car", PrintCarVertex.class));
        job2.addResource(this.getClass().getResource("/sample-pojo-1.0-car.jar"));
        job2.submit(dag2);

        Future f1 = job1.execute();
        Future f2 = job2.execute();

        assertCompletesEventually(f1);
        assertCompletesEventually(f2);

        f1.get();
        f2.get();
    }

    @Test
    public void test_Class_Isolation() throws Exception {
        factory.newInstances(new Config(), 3);
        HazelcastInstance client = factory.newHazelcastClient();
        final Job job1 = JetEngine.getJob(client, generateRandomString(10));
        final Job job2 = JetEngine.getJob(client, generateRandomString(10));

        DAG dag1 = new DAG();
        dag1.addVertex(createVertex("create and print person", PrintPersonVertex.class));
        URL gzipResource1 = this.getClass().getResource("/Person$Appereance.class.gz");
        File classFile1 = createClassFileFromGzip(gzipResource1, "Person$Appereance.class");
        job1.addResource(classFile1.toURI().toURL().openStream(), "com.sample.pojo.person.Person$Appereance", ResourceType.CLASS);
        job1.submit(dag1);


        DAG dag2 = new DAG();
        dag2.addVertex(createVertex("create and print car", PrintCarVertex.class));
        URL gzipResource2 = this.getClass().getResource("/Car.class.gz");
        File classFile2 = createClassFileFromGzip(gzipResource2, "Car.class");
        job2.addResource(classFile2.toURI().toURL().openStream(), "com.sample.pojo.car.Car", ResourceType.CLASS);

        job2.submit(dag2);

        Future f1 = job1.execute();
        Future f2 = job2.execute();

        assertCompletesEventually(f1);
        assertCompletesEventually(f2);
        f1.get();
        f2.get();
    }

    @Test
    public void test_Resource_Isolation() throws Exception {
        factory.newInstances(new Config(), 3);
        HazelcastInstance client = factory.newHazelcastClient();
        final Job job1 = JetEngine.getJob(client, generateRandomString(10));
        final Job job2 = JetEngine.getJob(client, generateRandomString(10));

        DAG dag1 = new DAG();
        dag1.addVertex(createVertex("gpl", GPL.class));
        job1.addResource(new URL("https://www.gnu.org/licenses/gpl-3.0.txt").openStream(), "gpl", ResourceType.DATA);
        job1.submit(dag1);


        DAG dag2 = new DAG();
        dag2.addVertex(createVertex("apache", Apache.class));
        job2.addResource(new URL("http://www.apache.org/licenses/LICENSE-2.0.txt").openStream(), "apache", ResourceType.DATA);
        job2.submit(dag2);

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
