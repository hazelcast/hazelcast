/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.TestProcessors.ListSource;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.combineP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Processors_globalAggregationIntegrationTest extends JetTestSupport {

    @Parameter
    public boolean singleStageProcessor;

    @Parameters(name = "singleStageProcessor={0}")
    public static Collection<Object> parameters() {
        return asList(true, false);
    }

    @Test
    public void smokeTest() throws Exception {
        runTest(asList(1L, 2L), 3L);
    }

    private void runTest(List<Long> sourceItems, Long expectedOutput)
            throws Exception {
        HazelcastInstance instance = createHazelcastInstance();

        AggregateOperation1<Long, ?, Long> summingOp = summingLong((Long l) -> l);

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", () -> new ListSource(sourceItems)).localParallelism(1);
        Vertex sink = dag.newVertex("sink", writeListP("sink"));

        if (singleStageProcessor) {
            Vertex aggregate = dag.newVertex("aggregate", Processors.aggregateP(summingOp))
                    .localParallelism(1);
            dag
                    .edge(between(source, aggregate).distributed().allToOne("foo"))
                    .edge(between(aggregate, sink).isolated());

        } else {
            Vertex accumulate = dag.newVertex("accumulate", Processors.accumulateP(summingOp));
            Vertex combine = dag.newVertex("combine", combineP(summingOp)).localParallelism(1);
            dag
                    .edge(between(source, accumulate))
                    .edge(between(accumulate, combine).distributed().allToOne("foo"))
                    .edge(between(combine, sink).isolated());
        }

        instance.getJet().newJob(dag).join();

        IList<Long> sinkList = instance.getList("sink");

        assertEquals(singletonList(expectedOutput), new ArrayList<>(sinkList));
        // wait a little more and make sure, that there are no more frames
        Thread.sleep(1000);
        assertEquals(singletonList(expectedOutput), new ArrayList<>(sinkList));

        assertEquals(expectedOutput, sinkList.get(0));
    }
}
