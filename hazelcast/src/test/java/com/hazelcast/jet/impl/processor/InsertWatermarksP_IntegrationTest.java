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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.TestProcessors.ListSource;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.TestProcessors.MapWatermarksToString.mapWatermarksToString;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingLag;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InsertWatermarksP_IntegrationTest extends JetTestSupport {

    private HazelcastInstance instance;

    @Before
    public void before() {
        instance = super.createHazelcastInstance();
    }

    @Test
    public void test() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", ListSource.supplier(asList(111L, 222L, 333L)));
        Vertex iwm = dag.newVertex("iwm", Processors.insertWatermarksP(eventTimePolicy(
                (Long x) -> x, limitingLag(100), 100, 0, 0)))
                .localParallelism(1);
        Vertex mapWmToStr = dag.newVertex("mapWmToStr", mapWatermarksToString(false))
                .localParallelism(1);
        Vertex sink = dag.newVertex("sink", writeListP("list"));

        dag.edge(between(source, iwm))
           .edge(between(iwm, mapWmToStr))
           .edge(between(mapWmToStr, sink));

        instance.getJet().newJob(dag).join();

        Object[] actual = instance.getList("list").toArray();
        assertArrayEquals(Arrays.toString(actual), new Object[]{"wm(0)", 111L, "wm(100)", 222L, "wm(200)", 333L}, actual);
    }
}
