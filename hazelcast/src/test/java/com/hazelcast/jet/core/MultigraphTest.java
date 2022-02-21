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

package com.hazelcast.jet.core;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.TestProcessors.ListSource;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.core.Edge.from;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultigraphTest extends JetTestSupport {

    @Test
    public void test() {
        DAG dag = new DAG();
        List<Integer> input = IntStream.range(0, 200_000).boxed().collect(Collectors.toList());
        Vertex source = dag.newVertex("source", ListSource.supplier(input));
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP("sink"));
        dag.edge(from(source, 0).to(sink, 0));
        dag.edge(from(source, 1).to(sink, 1).partitioned(wholeItem()).distributed());

        HazelcastInstance instance = createHazelcastInstance();
        createHazelcastInstance();
        instance.getJet().newJob(dag).join();

        int numMembers = 2;
        long numEdges = 2;
        assertEquals(input.stream().collect(toMap(identity(), v -> numMembers * numEdges)),
                instance.getList("sink").stream().collect(Collectors.groupingBy(identity(), Collectors.counting())));
    }
}
