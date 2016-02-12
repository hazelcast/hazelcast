package com.hazelcast.jet;

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.base.JetBaseTest;
import com.hazelcast.jet.impl.dag.EdgeImpl;
import com.hazelcast.jet.impl.strategy.IListBasedShufflingStrategy;
import com.hazelcast.jet.processors.DummyProcessor;
import com.hazelcast.jet.processors.ListProcessor;
import com.hazelcast.jet.processors.ReverseProcessor;
import com.hazelcast.jet.spi.dag.DAG;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class SimpleMultiNodeTestSuite extends JetBaseTest {
    @BeforeClass
    public static void initCluster() throws Exception {
        JetBaseTest.initCluster(3);
    }

    @Test
    public void shufflingListTest() throws Exception {
        System.out.println(System.nanoTime() + " --> shufflingListTest");
        Application application = createApplication("shufflingListTest");
        IList targetList = SERVER.getList("target.shufflingListTest");

        try {
            DAG dag = createDAG();

            int CNT = 100;

            fillMap("source.shufflingListTest", SERVER, CNT);

            Vertex vertex1 = createVertex("MapReader", DummyProcessor.Factory.class);
            Vertex vertex2 = createVertex("Sorter", ListProcessor.Factory.class, 1);

            addVertices(dag, vertex1, vertex2);

            vertex1.addSourceMap("source.shufflingListTest");
            vertex2.addSinkList("target.shufflingListTest");

            addEdges(
                    dag,
                    new EdgeImpl.EdgeBuilder("edge", vertex1, vertex2).
                            shuffling(true).
                            shufflingStrategy(
                                    new IListBasedShufflingStrategy("target.shufflingListTest")
                            ).
                            build()
            );

            executeApplication(dag, application).get(TIME_TO_AWAIT, TimeUnit.SECONDS);
            assertEquals(CNT, targetList.size());
        } finally {
            SERVER.getMap("source.shufflingListTest").clear();
            SERVER.getMap("target.shufflingListTest").clear();
            application.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
        }
    }

    @Test
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
