/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map.Entry;

import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.jet.function.DistributedFunctions.alwaysTrue;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
public class DotTest {

    @Test
    public void when_dagToDotString() {
        DAG dag = new DAG();
        Vertex a = dag.newVertex("a", noopP());
        Vertex b = dag.newVertex("b", noopP());
        Vertex c = dag.newVertex("c", noopP());
        Vertex d = dag.newVertex("d", noopP());

        dag.edge(from(a, 0).to(c, 0).partitioned(wholeItem()));
        dag.edge(from(a, 1).to(b, 0).broadcast().distributed());

        // can't assert, contains multiple subgraphs, order isn't stable
        System.out.println(dag.toDotString());
    }

    @Test
    public void when_pipelineToDotString() {
        Pipeline p = Pipeline.create();
        BatchStage<Entry> source = p.drawFrom(Sources.map("source1"));

        source
                .addKey(Entry::getKey)
                .aggregate(AggregateOperations.counting())
                .setName("aggregateToCount")
                .drainTo(Sinks.logger());

        source
                .addKey(Entry::getKey)
                .aggregate(AggregateOperations.toSet())
                .setName("aggregateToSet")
                .drainTo(Sinks.logger());

        source.filter(alwaysTrue())
              .drainTo(Sinks.logger());

        // can't assert, contains multiple sub-paths, order isn't stable
        System.out.println(p.toDotString());
        System.out.println(p.toDag().toDotString());
    }

    @Test
    public void when_snapshotVertex_then_skip() {
        DAG dag = new DAG();
        Vertex snapshotA = dag.newVertex("__snapshot_read.a", noopP());
        Vertex snapshotB = dag.newVertex("__snapshot_read.b", noopP());
        Vertex a = dag.newVertex("a", noopP());
        Vertex b = dag.newVertex("b", noopP());

        dag.edge(from(a, 0).to(b, 0).broadcast().distributed());

        dag.edge(from(snapshotA, 0).to(a, 0).partitioned(wholeItem()));
        dag.edge(from(snapshotB, 0).to(b, 1).broadcast().distributed());

        assertFalse("snapshot vertex should not be in output", dag.toDotString().contains("snapshot"));
    }

    @Test
    public void assertedTest() {
        Pipeline p = Pipeline.create();
        // " in vertex name should be escaped
        p.drawFrom(Sources.map("source1\""))
         .addKey(Entry::getKey)
         .aggregate(AggregateOperations.counting())
         .setName("aggregateToCount")
         .drainTo(Sinks.logger());

        assertEquals("digraph Pipeline {\n" +
                "\t\"mapSource(source1\\\")\" -> \"aggregateToCount\";\n" +
                "\t\"aggregateToCount\" -> \"loggerSink\";\n" +
                "}", p.toDotString());
        assertEquals("digraph DAG {\n" +
                "\t\"mapSource(source1\\\")\" -> \"aggregateToCount-step1\" [label=\"partitioned\"];\n" +
                "\tsubgraph cluster_0 {\n" +
                "\t\t\"aggregateToCount-step1\" -> \"aggregateToCount-step2\" [label=\"distributed-partitioned\"];\n" +
                "\t}\n" +
                "\t\"aggregateToCount-step2\" -> \"loggerSink\";\n" +
                "}", p.toDag().toDotString());
    }
}
