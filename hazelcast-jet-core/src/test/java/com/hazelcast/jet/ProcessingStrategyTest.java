package com.hazelcast.jet;

import com.hazelcast.core.IList;
import com.hazelcast.jet.application.Application;
import com.hazelcast.jet.base.JetBaseTest;
import com.hazelcast.jet.impl.counters.LongCounter;
import com.hazelcast.jet.dag.EdgeImpl;
import com.hazelcast.jet.processors.CounterProcessor;
import com.hazelcast.jet.processors.DummyProcessor;
import com.hazelcast.jet.container.CounterKey;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.strategy.ProcessingStrategy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ProcessingStrategyTest extends JetBaseTest {

    @BeforeClass
    public static void initCluster() throws Exception {
        JetBaseTest.initCluster(3);
    }

    @Test
    public void testRoundRobin() throws Exception {
        int itemCount = 100;
        int taskCount = 4;
        Application application = test(itemCount, ProcessingStrategy.ROUND_ROBIN, taskCount);
        Map<CounterKey, Accumulator> accumulatorMap = application.getAccumulators();

        int total = 0;
        for (Accumulator accumulator : accumulatorMap.values()) {
            LongCounter longCounter = (LongCounter) accumulator;
            total += longCounter.getLocalValue();
        }
        assertEquals(itemCount, total);
        application.finalizeApplication();

    }

    @Test
    public void testBroadcast() throws Exception {
        int itemCount = 100;
        int taskCount = 4;
        Application application = test(itemCount, ProcessingStrategy.BROADCAST, taskCount);
        Map<CounterKey, Accumulator> accumulatorMap = application.getAccumulators();

        int total = 0;
        for (Accumulator accumulator : accumulatorMap.values()) {
            LongCounter longCounter = (LongCounter) accumulator;
            total += longCounter.getLocalValue();
        }
        assertEquals(itemCount * taskCount, total);
        application.finalizeApplication();
    }

    private Application test(int count, ProcessingStrategy processingStrategy, int taskCount) throws Exception {
        String applicationName = generateRandomString(10);
        Application application = createApplication(applicationName);
        DAG dag = createDAG(applicationName);

        insertToList(applicationName, count);
        Vertex vertex1 = createVertex("Dummy", DummyProcessor.Factory.class);
        Vertex vertex2 = createVertex("Counter", CounterProcessor.CounterProcessorFactory.class, taskCount);

        addVertices(dag, vertex1, vertex2);

        vertex1.addSourceList(applicationName);

        addEdges(
                dag,
                new EdgeImpl.EdgeBuilder("edge", vertex1, vertex2).
                        processingStrategy(processingStrategy)
                        .build()
        );

        executeApplication(dag, application).get(TIME_TO_AWAIT, TimeUnit.SECONDS);
        return application;
    }

    private void insertToList(String applicationName, int count) {
        IList<Object> list = SERVER.getList(applicationName);
        for (int i = 0; i < count; i++) {
            list.add(i);
        }
    }
}
