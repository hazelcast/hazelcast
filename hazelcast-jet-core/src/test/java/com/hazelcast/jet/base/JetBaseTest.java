package com.hazelcast.jet.base;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.impl.dag.DAGImpl;
import com.hazelcast.jet.impl.dag.VertexImpl;
import com.hazelcast.jet.impl.hazelcast.JetEngine;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.spi.config.JetConfig;
import com.hazelcast.jet.spi.dag.DAG;
import com.hazelcast.jet.spi.dag.Edge;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.processor.ProcessorDescriptor;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestEnvironment;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.AfterClass;

public abstract class JetBaseTest extends HazelcastTestSupport {
    public static final int TIME_TO_AWAIT = 600;
    public static final String TEST_DATA_PATH = "test.data.path";
    private static final AtomicInteger APPLICATION_NAME_COUNTER = new AtomicInteger();
    protected static JetApplicationConfig JETCONFIG;
    protected static JetConfig CONFIG;
    protected static HazelcastInstance SERVER;
    protected static HazelcastInstance CLIENT;
    protected static List<File> createdFiles = new ArrayList<File>();
    protected static TestHazelcastFactory HAZELCAST_FACTORY;
    protected static HazelcastInstance[] HAZELCAST_INSTANCES;

    static {
        System.setProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK, "false");
    }

    public static void initCluster(int membersCount) throws Exception {
        JETCONFIG = new JetApplicationConfig("testApplication");
        JETCONFIG.setJetSecondsToAwait(100000);
        JETCONFIG.setChunkSize(4000);
        JETCONFIG.setMaxProcessingThreads(
                Runtime.getRuntime().availableProcessors());

        System.setProperty(TEST_DATA_PATH, "src/test/resources/data/");

        CONFIG = new JetConfig();
        CONFIG.addJetApplicationConfig(JETCONFIG);
        CONFIG.getMapConfig("source").setInMemoryFormat(InMemoryFormat.OBJECT);

        HAZELCAST_FACTORY = new TestHazelcastFactory();
        buildCluster(membersCount);
        warmUpPartitions(SERVER);
    }

    protected static void buildCluster(int memberCount) throws Exception {
        HAZELCAST_INSTANCES = new HazelcastInstance[memberCount];

        for (int i = 0; i < memberCount; i++) {
            HAZELCAST_INSTANCES[i] = HAZELCAST_FACTORY.newHazelcastInstance(CONFIG);
        }

        SERVER = HAZELCAST_INSTANCES[0];
        CLIENT = HAZELCAST_FACTORY.newHazelcastClient();
    }

    public static Vertex createVertex(String name, Class processorClass, int taskCount) {
        return new VertexImpl(
                name,
                ProcessorDescriptor.
                        builder(processorClass).
                        withTaskCount(taskCount).
                        build()
        );
    }

    public static Vertex createVertex(String name, Class processorClass) {
        return createVertex(name, processorClass, Runtime.getRuntime().availableProcessors());
    }

    @AfterClass
    public static void after() {
        try {
            HAZELCAST_FACTORY.terminateAll();
        } finally {
            try {
                for (File f : createdFiles) {
                    if (f != null) {
                        f.delete();
                    }
                }
            } finally {
                createdFiles.clear();
            }
        }
    }

    protected Application createApplication() {
        return createApplication("testApplication " + APPLICATION_NAME_COUNTER.incrementAndGet());
    }

    protected Application createApplication(String applicationName) {
        return JetEngine.getJetApplication(SERVER, applicationName, JETCONFIG);
    }

    protected DAG createDAG() {
        return createDAG("testDagApplication");
    }

    protected DAG createDAG(String dagName) {
        return new DAGImpl(dagName);
    }

    protected void fillMap(String source, HazelcastInstance instance, int CNT) throws Exception {
        final IMap<Integer, String> sourceMap = instance.getMap(source);

        List<Future> l = new ArrayList<Future>();

        for (int i = 1; i <= CNT; i++) {
            l.add(sourceMap.putAsync(i, String.valueOf(i)));
        }

        for (Future f : l) {
            f.get();
        }
    }

    public Future executeApplication(DAG dag, Application application) throws Exception {
        application.submit(dag);
        return application.execute();
    }

    protected void addVertices(DAG dag, Vertex... vertices) {
        for (Vertex vertex : vertices) {
            dag.addVertex(vertex);
        }
    }

    protected void addEdges(DAG dag, Edge edge) {
        dag.addEdge(edge);
    }

    protected String createDataFile(int recordsCount, String file) throws Exception {
        String inputPath = System.getProperty(TEST_DATA_PATH);

        File f = new File(inputPath + file);
        this.createdFiles.add(f);

        FileWriter fw = new FileWriter(f);
        StringBuilder sb = new StringBuilder();

        for (int i = 1; i <= recordsCount; i++) {
            sb.append(String.valueOf(i)).append(" \n");

            if (i % 4096 == 0) {
                fw.write(sb.toString());
                fw.flush();
                sb = new StringBuilder();
            }
        }

        if (sb.length() > 0) {
            fw.write(sb.toString());
            fw.flush();
            fw.close();
        }

        return f.getAbsolutePath();
    }

    protected String touchFile(String file) {
        String inputPath = System.getProperty(TEST_DATA_PATH);
        File f = new File(inputPath + file);
        this.createdFiles.add(f);
        return f.getAbsolutePath();
    }

    protected void fillMapWithDataFromFile(HazelcastInstance hazelcastInstance, String mapName, String file)
            throws Exception {
        IMap<String, String> map = hazelcastInstance.getMap(mapName);
        String inputPath = System.getProperty(TEST_DATA_PATH);

        LineNumberReader reader = new LineNumberReader(
                new FileReader(new File(inputPath + file))
        );

        try {
            String line;
            List<Future> futures = new ArrayList<Future>();
            StringBuilder sb = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }

            futures.add(map.putAsync(file, sb.toString()));

            for (Future ff : futures) {
                ff.get();
            }
        } finally {
            reader.close();
        }
    }

    protected long fileLinesCount(String sinkFile) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(sinkFile));
        try {
            byte[] c = new byte[1024];

            long count = 0;
            int readChars;
            boolean empty = true;

            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }

    protected void printException(Exception e) {
        e.printStackTrace(System.out);
    }
}
