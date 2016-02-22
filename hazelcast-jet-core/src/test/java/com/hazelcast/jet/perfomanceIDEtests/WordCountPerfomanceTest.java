package com.hazelcast.jet.perfomanceIDEtests;

import com.hazelcast.jet.spi.dag.DAG;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.base.JetBaseTest;
import com.hazelcast.jet.impl.dag.EdgeImpl;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.api.application.Application;
import com.hazelcast.jet.spi.strategy.HashingStrategy;
import com.hazelcast.jet.spi.strategy.ProcessingStrategy;
import com.hazelcast.jet.processors.WordCounterProcessor;
import com.hazelcast.jet.processors.WordGeneratorProcessor;
import com.hazelcast.jet.spi.container.ContainerDescriptor;


import org.junit.Test;
import org.junit.Ignore;
import org.junit.BeforeClass;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@Ignore
public class WordCountPerfomanceTest extends JetBaseTest {
    @BeforeClass
    public static void initCluster() throws Exception {
        JetBaseTest.initCluster(1);
    }

    @Test
    public void wordCountFileTest() throws Exception {
        int CNT = 1024 * 1024 * 10;
        String fileName = "file";
        String sourceFile = createDataFile(CNT, fileName);
        Vertex vertex1 = createVertex("wordGenerator", WordGeneratorProcessor.Factory.class);
        Vertex vertex2 = createVertex("wordCounter", WordCounterProcessor.Factory.class);
        Application application = createApplication();
        try {
            DAG dag = createDAG();
            addVertices(dag, vertex1, vertex2);
            vertex1.addSourceFile(sourceFile);
            String sinkFile = touchFile("perfomance.wordCountFileTest.result");
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
            long t = System.currentTimeMillis();
            executeApplication(dag, application).get(TIME_TO_AWAIT, TimeUnit.SECONDS);
            System.out.println("Time=" + (System.currentTimeMillis() - t));
            assertEquals(CNT, fileLinesCount(sinkFile));
        } finally {
            application.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
        }
    }
}