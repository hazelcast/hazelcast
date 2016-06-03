/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IList;
import com.hazelcast.jet.application.Application;
import com.hazelcast.jet.base.JetBaseTest;
import com.hazelcast.jet.dag.EdgeImpl;
import com.hazelcast.jet.strategy.IListBasedShufflingStrategy;
import com.hazelcast.jet.processors.CombinerProcessor;
import com.hazelcast.jet.processors.CountProcessor;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Edge;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.Repeat;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class ShufflingLogicalTests extends JetBaseTest {
    @BeforeClass
    public static void initCluster() throws Exception {
        JetBaseTest.initCluster(2);
    }

    @Test
    @Repeat(100)
    public void counterCombinerListTest() throws Exception {
        Application application = createApplication("counterCombinerListTest");

        IList<Integer> sourceList = SERVER.getList(randomName());
        IList targetList = SERVER.getList(randomName());

        try {
            DAG dag = createDAG();

            int CNT = 10_000;

            for (int i = 0; i < CNT; i++) {
                sourceList.add(i);
            }

            Vertex counter = createVertex("counter", CountProcessor.Factory.class, 1);
            Vertex combiner = createVertex("combiner", CombinerProcessor.Factory.class, 1);
            addVertices(dag, counter, combiner);

            Edge edge = new EdgeImpl.EdgeBuilder("edge", counter, combiner)
                    .shuffling(true)
                    .shufflingStrategy(new IListBasedShufflingStrategy(targetList.getName()))
                    .build();

            addEdges(dag, edge);

            counter.addSourceList(sourceList.getName());
            combiner.addSinkList(targetList.getName());
            executeApplication(dag, application).get(TIME_TO_AWAIT, TimeUnit.SECONDS);
            assertEquals(1, targetList.size());
            System.out.println("targetList.get(0)=" + targetList.get(0));
            assertEquals(CNT, targetList.get(0));
        } finally {

            application.finalizeApplication().get(TIME_TO_AWAIT, TimeUnit.SECONDS);
        }
    }
}
