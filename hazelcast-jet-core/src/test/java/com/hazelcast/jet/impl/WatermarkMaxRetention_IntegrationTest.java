/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.core.IList;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.TestProcessors.ListSource;
import com.hazelcast.jet.core.TestProcessors.StuckForeverSourceP;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class WatermarkMaxRetention_IntegrationTest extends JetTestSupport {

    private static final String SINK_NAME = "sink";
    private JetInstance instance = createJetMember();

    @Test
    public void test_onEdgeCoalescing() {
        DAG dag = new DAG();
        // a vertex with two processor instances, one will emit a wm and the other won't
        Vertex source = dag.newVertex("source", (int count) -> asList(
                new ListSource(singletonList(new Watermark(1))),
                new StuckForeverSourceP()
        )).localParallelism(2);
        Vertex map = dag.newVertex("map", MapWmToStringP::new).localParallelism(1);
        Vertex sink = dag.newVertex("sink", writeListP(SINK_NAME));

        dag.edge(between(source, map))
           .edge(between(map, sink));

        doTest(dag);
    }

    @Test
    public void test_onVertexCoalescing() {
        DAG dag = new DAG();
        Vertex source0 = dag.newVertex("source0",
                () -> new ListSource(singletonList(new Watermark(1)))).localParallelism(1);
        Vertex source1 = dag.newVertex("source1", StuckForeverSourceP::new);
        // a vertex with two inputs, one will emit a wm and the other won't
        Vertex map = dag.newVertex("map", MapWmToStringP::new).localParallelism(1);
        Vertex sink = dag.newVertex("sink", writeListP(SINK_NAME));

        dag.edge(from(source0).to(map, 0))
           .edge(from(source1).to(map, 1))
           .edge(between(map, sink));

        doTest(dag);
    }

    private void doTest(DAG dag) {
        IList list = instance.getList(SINK_NAME);
        instance.newJob(dag, new JobConfig()
                .setMaxWatermarkRetainMillis(3000));

        assertTrueAllTheTime(() -> assertEquals(list.toString(), 0, list.size()), 1);

        assertTrueEventually(() -> {
            assertEquals(1, list.size());
            assertEquals("wm(1)", list.get(0));
        }, 4);
    }

    private static final class MapWmToStringP extends AbstractProcessor {
        @Override
        public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
            return tryEmit("wm(" + watermark.timestamp() + ')');
        }
    }
}
