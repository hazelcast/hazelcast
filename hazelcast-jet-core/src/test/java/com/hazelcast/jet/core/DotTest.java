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
import java.util.regex.Pattern;

import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.jet.function.DistributedPredicate.alwaysTrue;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.impl.pipeline.transform.AggregateTransform.FIRST_STAGE_VERTEX_NAME_SUFFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

        String actual = dag.toDotString();
        System.out.println(actual);
        // contains multiple subgraphs, order isn't stable, we'll assert individual lines and the length
        assertTrue(actual.startsWith("digraph DAG {"));
        assertTrue(actual.contains("\"a\" -> \"c\" [label=\"partitioned\"];"));
        assertTrue(actual.contains("\"a\" -> \"b\" [label=\"distributed-broadcast\"]"));
        assertTrue(actual.contains("\"d\";"));
        assertTrue(actual.endsWith("\n}"));
        assertEquals(101, actual.length());
    }

    @Test
    public void when_pipelineToDotString() {
        Pipeline p = Pipeline.create();
        BatchStage<Entry> source = p.drawFrom(Sources.map("source1"));

        source
                .groupingKey(Entry::getKey)
                .aggregate(AggregateOperations.counting())
                .setName("aggregateToCount")
                .drainTo(Sinks.logger());

        source
                .groupingKey(Entry::getKey)
                .aggregate(AggregateOperations.toSet())
                .setName("aggregateToSet")
                .drainTo(Sinks.logger());

        source.filter(alwaysTrue())
              .drainTo(Sinks.logger());

        String actualPipeline = p.toDotString();
        assertEquals(actualPipeline, "digraph Pipeline {\n" +
                "\t\"mapSource(source1)\" -> \"aggregateToCount\";\n" +
                "\t\"mapSource(source1)\" -> \"aggregateToSet\";\n" +
                "\t\"mapSource(source1)\" -> \"filter\";\n" +
                "\t\"aggregateToCount\" -> \"loggerSink\";\n" +
                "\t\"aggregateToSet\" -> \"loggerSink-2\";\n" +
                "\t\"filter\" -> \"loggerSink-3\";\n" +
                "}");

        String actualDag = p.toDag().toDotString();
        System.out.println(actualDag);
        // contains multiple subgraphs, order isn't stable, we'll assert individual lines and the length
        assertTrue(actualDag.startsWith("digraph DAG {"));
        assertTrue(actualDag.contains("\"mapSource(source1)\" -> \"aggregateToCount" + FIRST_STAGE_VERTEX_NAME_SUFFIX
                + "\" [label=\"partitioned\"];"));
        assertTrue(actualDag.contains("\"mapSource(source1)\" -> \"filter\";"));
        assertTrue(actualDag.contains("\"mapSource(source1)\" -> \"aggregateToSet" + FIRST_STAGE_VERTEX_NAME_SUFFIX
                + "\" [label=\"partitioned\"];"));
        assertTrue(regexContains(actualDag, "subgraph cluster_[01] \\{\n" +
                "\t\t\"aggregateToCount" + FIRST_STAGE_VERTEX_NAME_SUFFIX
                        + "\" -> \"aggregateToCount\" \\[label=\"distributed-partitioned\"];\n" +
                "\t}"));

        assertTrue(regexContains(actualDag, "\"aggregateToCount\" -> \"loggerSink(-[23])?\";"));
        assertTrue(regexContains(actualDag, "subgraph cluster_[01] \\{\n" +
                "\t\t\"aggregateToSet" + FIRST_STAGE_VERTEX_NAME_SUFFIX + "\" -> \"aggregateToSet\" "
                        + "\\[label=\"distributed-partitioned\"];\n" +
                "\t}"));
        assertTrue(regexContains(actualDag, "\"aggregateToSet\" -> \"loggerSink(-[23])?\";"));
        assertTrue(regexContains(actualDag, "\"filter\" -> \"loggerSink(-[23])?\";"));
        assertTrue(actualDag.endsWith("\n}"));
    }

    private boolean regexContains(String str, String regex) {
        return Pattern.compile(regex).matcher(str).find();
    }

    @Test
    public void assertedTest() {
        Pipeline p = Pipeline.create();
        // " in vertex name should be escaped
        p.drawFrom(Sources.map("source1\""))
         .groupingKey(Entry::getKey)
         .aggregate(AggregateOperations.counting())
         .setName("aggregateToCount")
         .drainTo(Sinks.logger());

        assertEquals("digraph Pipeline {\n" +
                "\t\"mapSource(source1\\\")\" -> \"aggregateToCount\";\n" +
                "\t\"aggregateToCount\" -> \"loggerSink\";\n" +
                "}", p.toDotString());
        assertEquals("digraph DAG {\n" +
                "\t\"mapSource(source1\\\")\" -> \"aggregateToCount" + FIRST_STAGE_VERTEX_NAME_SUFFIX
                        + "\" [label=\"partitioned\"];\n" +
                "\tsubgraph cluster_0 {\n" +
                "\t\t\"aggregateToCount" + FIRST_STAGE_VERTEX_NAME_SUFFIX
                        + "\" -> \"aggregateToCount\" [label=\"distributed-partitioned\"];\n" +
                "\t}\n" +
                "\t\"aggregateToCount\" -> \"loggerSink\";\n" +
                "}", p.toDag().toDotString());
    }
}
