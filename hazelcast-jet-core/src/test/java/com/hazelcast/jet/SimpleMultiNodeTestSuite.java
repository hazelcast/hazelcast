package com.hazelcast.jet;

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.base.JetBaseTest;
import com.hazelcast.jet.impl.dag.EdgeImpl;
import com.hazelcast.jet.impl.dag.tap.sink.HazelcastListPartitionWriter;
import com.hazelcast.jet.impl.strategy.IListBasedShufflingStrategy;
import com.hazelcast.jet.processors.DummyProcessor;
import com.hazelcast.jet.processors.DummyProcessorForShufflingList;
import com.hazelcast.jet.processors.ListProcessor;
import com.hazelcast.jet.processors.ReverseProcessor;
import com.hazelcast.jet.spi.dag.DAG;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.strategy.ShufflingStrategy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.Repeat;
import com.hazelcast.test.annotation.SlowTest;

import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@Category(SlowTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class SimpleMultiNodeTestSuite extends JetBaseTest {
    @BeforeClass
    public static void initCluster() throws Exception {
        JetBaseTest.initCluster(3);
    }

    @Test
    @Repeat(500)
    public void simpleList2ListTest() throws Exception {
        Application application = createApplication("simpleList2ListTest");

        IList sourceList = SERVER.getList(randomName());
        IList targetList = SERVER.getList(randomName());

        try {
            DAG dag = createDAG();

            int CNT = 1;

            for (int i = 0; i < CNT; i++) {
                sourceList.add(i);
            }

            Vertex vertex = createVertex("Dummy", DummyProcessor.Factory.class, 1);
            addVertices(dag, vertex);
            vertex.addSourceList(sourceList.getName());
            vertex.addSinkList(targetList.getName());
            executeApplication(dag, application).get(TIME_TO_AWAIT, TimeUnit.SECONDS);
            assertEquals(CNT, targetList.size());
        } finally {
            application.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
        }
    }

    @Test
    @Repeat(1000)
    public void shufflingListTest() throws Exception {
        System.out.println(System.nanoTime() + " --> shufflingListTest");
        Application application = createApplication("shufflingListTest");
        IList targetList = SERVER.getList(randomName());
        IMap sourceMap = SERVER.getMap(randomName());
        
        try {
            DAG dag = createDAG();

            int CNT = 100;

            fillMap(sourceMap.getName(), SERVER, CNT);

            Vertex vertex1 = createVertex("MapReader", DummyProcessorForShufflingList.Factory.class);
            Vertex vertex2 = createVertex("Sorter", ListProcessor.Factory.class, 1);

            addVertices(dag, vertex1, vertex2);

            vertex1.addSourceMap(sourceMap.getName());
            vertex2.addSinkList(targetList.getName());

            addEdges(
                    dag,
                    new EdgeImpl.EdgeBuilder("edge", vertex1, vertex2).
                            shuffling(true).
                            shufflingStrategy(
                                    new IListBasedShufflingStrategy("target.shufflingListTest")
                            ).
                            build()
            );

            ListProcessor.DEBUG_COUNTER.set(0);
            DummyProcessorForShufflingList.DEBUG_COUNTER.set(0);
            HazelcastListPartitionWriter.DEBUG_COUNTER.set(0);
            HazelcastListPartitionWriter.DEBUG_COUNTER1.set(0);

            executeApplication(dag, application).get(TIME_TO_AWAIT, TimeUnit.SECONDS);

            assertEquals(CNT, DummyProcessorForShufflingList.DEBUG_COUNTER.get());
            assertEquals(CNT, ListProcessor.DEBUG_COUNTER.get());
            assertEquals(
                    "DummyProcessor.DEBUG_COUNTER=" + DummyProcessorForShufflingList.DEBUG_COUNTER.get() +
                            "ListProcessor.DEBUG_COUNTER" + ListProcessor.DEBUG_COUNTER.get() +
                            " HazelcastListPartitionWriter.DEBUG_COUNTER=" + HazelcastListPartitionWriter.DEBUG_COUNTER.get() +
                            " HazelcastListPartitionWriter.DEBUG_COUNTER1=" + HazelcastListPartitionWriter.DEBUG_COUNTER1.get()
                    ,
                    CNT,
                    targetList.size()
            );
        } finally {
            sourceMap.clear();
            targetList.clear();
            application.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
        }
    }

    @Test
    @Repeat(500)
    public void mapReverserTest() throws Exception {
        System.out.println(System.nanoTime() + " --> mapReverserTest");
        Application application = createApplication("mapReverserTest");
        IMap targetMap = SERVER.getMap("target.mapReverserTest");

        try {
            DAG dag = createDAG();

            int CNT = 100;
            fillMap("source.mapReverserTest", SERVER, CNT);

            Vertex vertex = createVertex("reverser", ReverseProcessor.Factory.class, 1);

            vertex.addSourceMap("source.mapReverserTest");
            vertex.addSinkMap("target.mapReverserTest");

            addVertices(dag, vertex);
            executeApplication(dag, application).get(TIME_TO_AWAIT, TimeUnit.SECONDS);
            assertEquals(CNT, targetMap.size());
        } finally {
            SERVER.getMap("source.mapReverserTest").clear();
            SERVER.getMap("target.mapReverserTest").clear();
            application.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
        }
    }
}
