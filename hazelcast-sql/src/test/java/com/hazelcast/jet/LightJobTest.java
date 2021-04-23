/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.Edge.between;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
public class LightJobTest extends JetTestSupport {

    private JetInstance inst;
    private JetInstance client;

    @BeforeClass
    public static void beforeClass() {
        windowsTimerHack();
    }

    @Before
    public void before() {
        inst = createJetMember();
        createJetMember();
        client = createJetClient();
    }

    @Test
    public void test_member() {
        test(inst);
    }

    @Test
    public void test_client() {
        test(client);
    }

    private void test(JetInstance submittingInstance) {
        List<Integer> items = IntStream.range(0, 1_000).boxed().collect(Collectors.toList());

        DAG dag = new DAG();
        Vertex src = dag.newVertex("src", processorFromPipelineSource(TestSources.items(items)));
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP("sink"));
        dag.edge(between(src, sink).distributed());

        submittingInstance.newLightJob(dag).join();
        List<Integer> result = inst.getList("sink");
        assertThat(result).containsExactlyInAnyOrderElementsOf(items);
    }

    @Test
    public void test_cancellation_member() {
        test_cancellation(inst);
    }

    @Test
    public void test_cancellation_client() {
        test_cancellation(client);
    }

    private void test_cancellation(JetInstance submittingInstance) {
        DAG dag = new DAG();
        dag.newVertex("v", () -> new TestProcessors.MockP().streaming());

        LightJob job = submittingInstance.newLightJob(dag);
        // TODO fix the race if we cancel before the execution starts
        sleepSeconds(1);
        job.cancel();
        job.join();
    }

    @Test
    public void jetBench() throws Exception {
        int warmUpIterations = 100;
        int realIterations = 2000;
        DAG dag = new DAG();
        dag.newVertex("v", Processors.noopP());
        logger.info("will submit " + warmUpIterations + " jobs");
        for (int i = 0; i < warmUpIterations; i++) {
            inst.newLightJob(dag).join();
        }
//        for (int i = 20; i >= 0; i--) {
//            System.out.println("attach profiler " + i);
//            Thread.sleep(1000);
//        }
        logger.info("warmup jobs done, starting benchmark");
        long start = System.nanoTime();
        for (int i = 0; i < realIterations; i++) {
            inst.newLightJob(dag).join();
        }
        long elapsedMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
        System.out.println(realIterations + " jobs run in " + (elapsedMicros / realIterations) + " us/job");
    }

    @Test
    public void sqlBench() {
        int warmUpIterations = 100;
        int realIterations = 200;
        SqlService sqlService = inst.getSql();
        logger.info("will submit " + warmUpIterations + " jobs");
        inst.getMap("m").put(1, 1);
        int numRows = 0;
        for (int i = 0; i < warmUpIterations; i++) {
            for (SqlRow r : sqlService.execute("select * from m")) {
                numRows++;
            }
        }
        logger.info("warmup jobs done, starting benchmark");
        long start = System.nanoTime();
        for (int i = 0; i < realIterations; i++) {
            for (SqlRow r : sqlService.execute("select * from m")) {
                numRows++;
            }
        }
        long elapsedMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
        System.out.println(numRows);
        System.out.println(realIterations + " queries run in " + (elapsedMicros / realIterations) + " us/job");
    }

    public static void windowsTimerHack() {
        Thread t = new Thread(() -> {
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) { // a delicious interrupt, omm, omm
            }
        });
        t.setDaemon(true);
        t.start();
    }
}
