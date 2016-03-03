package com.hazelcast.jet;


import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.base.JetBaseTest;
import com.hazelcast.jet.impl.counters.LongCounter;
import com.hazelcast.jet.impl.dag.DAGImpl;
import com.hazelcast.jet.impl.dag.EdgeImpl;
import com.hazelcast.jet.processors.CounterProcessor;
import com.hazelcast.jet.processors.DummyProcessor;
import com.hazelcast.jet.processors.VerySlowProcessor;
import com.hazelcast.jet.processors.WordCounterProcessor;
import com.hazelcast.jet.processors.WordGeneratorProcessor;
import com.hazelcast.jet.processors.WordSorterProcessor;
import com.hazelcast.jet.spi.container.ContainerDescriptor;
import com.hazelcast.jet.spi.container.CounterKey;
import com.hazelcast.jet.spi.counters.Accumulator;
import com.hazelcast.jet.spi.dag.DAG;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.strategy.HashingStrategy;
import com.hazelcast.jet.spi.strategy.ProcessingStrategy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class SimpleSingleNodeTestSuite extends JetBaseTest {
    @BeforeClass
    public static void initCluster() throws Exception {
        JetBaseTest.initCluster(1);
    }

    @Test
    public void interruptionTest() throws Exception {
        final Application application = createApplication("interruptionTest");
        try {
            DAG dag = createDAG();

            fillMap("source.interruptionTest", SERVER, 100_0000);

            Vertex vertex1 = createVertex("dummy1", VerySlowProcessor.Factory.class);
            Vertex vertex2 = createVertex("dummy2", VerySlowProcessor.Factory.class);

            vertex1.addSourceMap("source.interruptionTest");
            vertex2.addSinkMap("target");

            addVertices(dag, vertex1, vertex2);
            addEdges(dag, new EdgeImpl("edge", vertex1, vertex2));

            Future executionFuture = executeApplication(dag, application);

            final AtomicBoolean success = new AtomicBoolean(false);

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(1000);
                        application.interrupt().get();
                        success.set(true);
                    } catch (Exception e) {
                        printException(e);
                        success.set(false);
                    }
                }
            }).start();

            try {
                executionFuture.get(TIME_TO_AWAIT, TimeUnit.SECONDS);
                success.set(false);
            } catch (Throwable e) {
                success.set(true);
            }

            assertTrue(success.get());
        } finally {
            try {
                SERVER.getMap("source.interruptionTest").clear();
            } finally {
                application.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void complexGraphTest() throws Exception {
        final Application application = createApplication("complexGraphTest");

        try {
            DAG dag = createDAG();

            IMap<Integer, String> sinkMap1 = SERVER.getMap("sinkMap1.complexGraphTest");
            IMap<Integer, String> sinkMap2 = SERVER.getMap("sinkMap2.complexGraphTest");

            int CNT = 100_000;

            fillMap("sourceMap.complexGraphTest", SERVER, CNT);

            assertEquals(CNT, SERVER.getMap("sourceMap.complexGraphTest").size());

            Vertex root = createVertex("root", DummyProcessor.Factory.class);
            Vertex vertex11 = createVertex("v11", DummyProcessor.Factory.class);
            Vertex vertex21 = createVertex("v21", DummyProcessor.Factory.class);
            Vertex vertex12 = createVertex("v12", DummyProcessor.Factory.class);
            Vertex vertex22 = createVertex("v22", DummyProcessor.Factory.class);

            root.addSourceMap("sourceMap.complexGraphTest");
            vertex12.addSinkMap("sinkMap1.complexGraphTest");
            vertex22.addSinkMap("sinkMap2.complexGraphTest");

            addVertices(dag, root, vertex11, vertex12, vertex21, vertex22);

            addEdges(dag, new EdgeImpl("edge1", root, vertex11));
            addEdges(dag, new EdgeImpl("edge2", root, vertex21));

            addEdges(dag, new EdgeImpl("edge3", vertex11, vertex12));
            addEdges(dag, new EdgeImpl("edge4", vertex21, vertex22));

            executeApplication(dag, application).get(TIME_TO_AWAIT, TimeUnit.SECONDS);

            assertEquals(CNT, sinkMap1.size());
            assertEquals(CNT, sinkMap2.size());
        } finally {
            try {
                SERVER.getMap("sourceMap.complexGraphTest").clear();
            } finally {
                application.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
                ;
            }
        }
    }

    @Test
    public void giantGraphTest() throws Exception {
        final Application application = createApplication("giantGraphTest");
        int branchCount = 5;
        int vertexCount = 5;
        final int CNT = 10_000;

        try {
            DAG dag = createDAG();

            fillMap("sourceMap.giantGraphTest", SERVER, CNT);

            List<IMap<Integer, String>> sinks = new ArrayList<IMap<Integer, String>>(branchCount);

            for (int i = 1; i <= branchCount; i++) {
                sinks.add(SERVER.<Integer, String>getMap("sinkMap.giantGraphTest" + i));
            }

            Vertex root = createVertex("root", DummyProcessor.Factory.class);
            addVertices(dag, root);

            root.addSourceMap("sourceMap.giantGraphTest");

            for (int b = 1; b <= branchCount; b++) {
                Vertex last = root;
                for (int i = 1; i <= vertexCount; i++) {
                    Vertex vertex = createVertex("v_" + b + "_" + i, DummyProcessor.Factory.class);
                    addVertices(dag, vertex);

                    addEdges(dag, new EdgeImpl("e_" + b + "_" + i, last, vertex));

                    last = vertex;

                    if (i == vertexCount) {
                        vertex.addSinkMap("sinkMap.giantGraphTest" + b);
                    }
                }
            }

            executeApplication(dag, application).get(TIME_TO_AWAIT, TimeUnit.SECONDS);

            for (int i = 1; i <= branchCount; i++) {
                assertEquals(sinks.get(i - 1).size(), CNT);
            }
        } finally {
            try {
                SERVER.getMap("sourceMap.giantGraphTest").clear();

                for (int b = 1; b <= branchCount; b++) {
                    SERVER.getMap("sinkMap.giantGraphTest" + b).clear();
                }
            } finally {
                application.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
                ;
            }
        }
    }


    @Test
    public void serverMixingTest() throws Exception {
        mixing(
                SERVER, "serverMixingTest"
        );
    }

    @Test
    public void clientMixinTest() throws Exception {
        mixing(
                CLIENT, "clientMixinTest"
        );
    }

    private DAG createMixingDag(int idx, String applicationName) {
        DAG dag = new DAGImpl("dag-" + idx);

        Vertex vertex1 = createVertex("mod1", DummyProcessor.Factory.class, 1);
        Vertex vertex2 = createVertex("mod2", DummyProcessor.Factory.class, 1);

        vertex1.addSourceMap("source." + applicationName);
        vertex2.addSinkMap("target" + idx + "." + applicationName);

        dag.addVertex(vertex1);
        dag.addVertex(vertex2);
        dag.addEdge(new EdgeImpl("edge", vertex1, vertex2));
        return dag;
    }

    private void mixing(HazelcastInstance instance, String applicationName) throws Exception {
        Application application1 = createApplication(applicationName + ".1");
        Application application2 = createApplication(applicationName + ".2");
        Application application3 = createApplication(applicationName + ".3");
        Application application4 = createApplication(applicationName + ".4");

        try {
            final IMap<Integer, String> targetMap1 = SERVER.getMap("target1." + applicationName);
            final IMap<Integer, String> targetMap2 = SERVER.getMap("target2." + applicationName);
            final IMap<Integer, String> targetMap3 = SERVER.getMap("target3." + applicationName);
            final IMap<Integer, String> targetMap4 = SERVER.getMap("target4." + applicationName);

            int CNT = 100;

            fillMap("source." + applicationName, SERVER, CNT);

            DAG dag1 = createMixingDag(1, applicationName);
            DAG dag2 = createMixingDag(2, applicationName);
            DAG dag3 = createMixingDag(3, applicationName);
            DAG dag4 = createMixingDag(4, applicationName);

            application1.submit(dag1);
            application2.submit(dag2);
            application3.submit(dag3);
            application4.submit(dag4);

            List<Future> list = new ArrayList<Future>();

            for (int i = 0; i < 50; i++) {
                list.add(application1.execute());
                list.add(application2.execute());
                list.add(application3.execute());
                list.add(application4.execute());

                for (Future f : list) {
                    f.get();
                }

                list.clear();
                assertEquals(CNT, targetMap1.size());
                assertEquals(CNT, targetMap2.size());
                assertEquals(CNT, targetMap3.size());
                assertEquals(CNT, targetMap4.size());

                targetMap1.clear();
                targetMap2.clear();
                targetMap3.clear();
                targetMap4.clear();
            }
        } finally {
            application1.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
            application2.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
            application3.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
            application4.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
        }
    }

    @Test
    public void serverCounterTest() throws Exception {
        counterTest(
                SERVER, "serverCounterTest"
        );
    }

    @Test
    public void clientCounterTest() throws Exception {
        counterTest(
                CLIENT, "clientCounterTest"
        );
    }

    private void counterTest(HazelcastInstance instance, String testName) throws Exception {
        final Application application = createApplication(testName);

        try {
            DAG dag = createDAG();

            instance.getMap("target." + testName);
            int CNT = 100;

            fillMap("source." + testName, SERVER, CNT);

            Vertex vertex = createVertex("mod1", CounterProcessor.CounterProcessorFactory.class);
            vertex.addSourceMap("source." + testName);
            vertex.addSinkMap("target." + testName);
            addVertices(dag, vertex);

            executeApplication(dag, application).get(TIME_TO_AWAIT, TimeUnit.SECONDS);

            Map<CounterKey, Accumulator> accumulatorMap = application.getAccumulators();
            LongCounter longCounter = (LongCounter) accumulatorMap.values().iterator().next();
            assertEquals(longCounter.getPrimitiveValue(), CNT);
        } finally {
            application.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
            ;
        }
    }

    @Test
    public void wordCountFileSortedTest() throws Exception {
        final Application application = createApplication("wordCountFileSortedTest");
        try {
            DAG dag = createDAG();

            int CNT = 1024;
            String fileName = "file.wordCountFileSortedTest";
            String sourceFile = createDataFile(CNT, fileName);

            Vertex vertex1 = createVertex("wordGenerator", WordGeneratorProcessor.Factory.class);
            Vertex vertex2 = createVertex("wordCounter", WordCounterProcessor.Factory.class);
            Vertex vertex3 = createVertex("wordSorter", WordSorterProcessor.Factory.class, 1);

            addVertices(dag, vertex1, vertex2, vertex3);

            vertex1.addSourceFile(sourceFile);
            String sinkFile = touchFile("result.wordCountFileSortedTest");
            vertex3.addSinkFile(sinkFile);

            addEdges(dag, new EdgeImpl("edge1", vertex1, vertex2));
            addEdges(dag, new EdgeImpl("edge2", vertex2, vertex3));

            executeApplication(dag, application).get(TIME_TO_AWAIT, TimeUnit.SECONDS);

            assertEquals(CNT, fileLinesCount(sinkFile));
        } finally {
            application.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
            ;
        }
    }

    @Test
    public void wordCountFileTest() throws Exception {
        final Application application = createApplication("wordCountFileTest");

        try {
            DAG dag = createDAG();

            int CNT = 1024;
            String fileName = "file.wordCountFileTest";
            String sourceFile = createDataFile(CNT, fileName);

            Vertex vertex1 = createVertex("wordGenerator", WordGeneratorProcessor.Factory.class);
            Vertex vertex2 = createVertex("wordCounter", WordCounterProcessor.Factory.class);

            addVertices(dag, vertex1, vertex2);

            String sinkFile = touchFile("result.wordCountFileTest");

            vertex1.addSourceFile(sourceFile);
            vertex2.addSinkFile(sinkFile);

            addEdges(
                    dag,
                    new EdgeImpl.EdgeBuilder(
                            "edge",
                            vertex1,
                            vertex2
                    )
                            .processingStrategy(ProcessingStrategy.PARTITIONING)
                            .hashingStrategy(new HashingStrategy<String, String>() {
                                                 @Override
                                                 public int hash(String object,
                                                                 String partitionKey,
                                                                 ContainerDescriptor containerDescriptor) {
                                                     return partitionKey.hashCode();
                                                 }
                                             }
                            )
                            .partitioningStrategy(new PartitioningStrategy<String>() {
                                @Override
                                public String getPartitionKey(String key) {
                                    return key;
                                }
                            })
                            .build()
            );

            executeApplication(dag, application).get(TIME_TO_AWAIT, TimeUnit.SECONDS);
            assertEquals(CNT, fileLinesCount(sinkFile));
        } finally {
            application.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
            ;
        }
    }


    @Test
    public void wordCountMapTest() throws Exception {
        final Application application = createApplication("wordCountMapTest");

        try {
            DAG dag = createDAG();

            int CNT = 1024;

            String fileName = "file.wordCountMapTest";
            createDataFile(CNT, fileName);
            CONFIG.getMapConfig("wordtest.wordCountMapTest").setInMemoryFormat(InMemoryFormat.OBJECT);
            CONFIG.getMapConfig("wordresult.wordCountMapTest").setInMemoryFormat(InMemoryFormat.OBJECT);

            fillMapWithDataFromFile(SERVER, "wordtest.wordCountMapTest", fileName);

            Vertex vertex1 = createVertex("wordGenerator", WordGeneratorProcessor.Factory.class);
            Vertex vertex2 = createVertex("wordCounter", WordCounterProcessor.Factory.class);

            addVertices(dag, vertex1, vertex2);

            vertex1.addSourceMap("wordtest.wordCountMapTest");
            vertex2.addSinkMap("wordresult.wordCountMapTest");
            addEdges(dag, new EdgeImpl("edge", vertex1, vertex2));

            executeApplication(dag, application).get(TIME_TO_AWAIT, TimeUnit.SECONDS);

            IMap<String, AtomicInteger> result = SERVER.getMap("wordresult.wordCountMapTest");
            assertEquals(CNT, result.size());
        } finally {
            try {
                SERVER.getMap("wordtest.wordCountMapTest").clear();
            } finally {
                application.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
                ;
            }

        }
    }
}
