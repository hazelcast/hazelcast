package com.hazelcast.jet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.jet.application.Application;
import com.hazelcast.jet.container.CounterKey;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Edge;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.dag.tap.ListSink;
import com.hazelcast.jet.dag.tap.ListSource;
import com.hazelcast.jet.impl.counters.LongCounter;
import com.hazelcast.jet.strategy.ProcessingStrategy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ProcessingStrategyTest extends JetTestSupport {

    private static final int NODE_COUNT = 3;
    private static final int COUNT = 10_000;
    private static HazelcastInstance instance;

    @BeforeClass
    public static void initCluster() throws Exception {
        instance = createCluster(NODE_COUNT);
    }

    @Test
    public void testRoundRobin() throws Exception {
        int count = getCountWithStrategy(ProcessingStrategy.ROUND_ROBIN);
        assertEquals(count, COUNT);
    }

    @Test
    public void testBroadcast() throws Exception {
        int  count = getCountWithStrategy(ProcessingStrategy.BROADCAST);
        assertEquals(count, COUNT * TASK_COUNT);
    }

    private int getCountWithStrategy(ProcessingStrategy processingStrategy) throws Exception {
        Application application = JetEngine.getJetApplication(instance, processingStrategy.toString());

        IList<Integer> source = getList(instance);
        IList<Integer> sink = getList(instance);
        fillListWithInts(source, COUNT);

        DAG dag = new DAG();

        Vertex producer = createVertex("producer", TestProcessors.Noop.class);
        Vertex consumer = createVertex("consumer", TestProcessors.Noop.class);

        dag.addVertex(producer);
        dag.addVertex(consumer);
        producer.addSource(new ListSource(source));
        consumer.addSink(new ListSink(sink));

        dag.addEdge(new Edge.EdgeBuilder("edge", producer, consumer).
                processingStrategy(processingStrategy)
                .build());

        application.submit(dag);
        execute(application);

        return sink.size();
    }
}
