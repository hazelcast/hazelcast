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

package com.hazelcast.jet2;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet2.impl.ListConsumer;
import com.hazelcast.jet2.impl.ListProducer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class JetEngineTest extends HazelcastTestSupport {

    private static TestHazelcastInstanceFactory factory;

    @BeforeClass
    public static void before() {
        factory = new TestHazelcastInstanceFactory();
    }

    @AfterClass
    public static void after() {
        factory.shutdownAll();
    }

    @Test
    public void test() {
        HazelcastInstance instance = factory.newHazelcastInstance();
        JetEngine jetEngine = JetEngine.get(instance, "jetEngine");

        List<Integer> source = IntStream.range(0, 1000).boxed().collect(Collectors.toList());
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", () -> new ListProducer(source, 1024))
                .parallelism(1);

        ListConsumer listConsumer = new ListConsumer();
        Vertex consumer = new Vertex("consumer", () -> listConsumer)
                .parallelism(1);

        dag.addVertex(producer);
        dag.addVertex(consumer);

        dag.addEdge(new Edge(producer, consumer));
        Job job = jetEngine.newJob(dag);

        job.execute();

        System.out.println(listConsumer.getList());
    }

}
