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
import com.hazelcast.jet2.impl.AbstractProcessor;
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
import java.util.function.Function;
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

        List<Integer> evens = IntStream.range(0, 10).filter(f -> f % 2 == 0).
                boxed().collect(Collectors.toList());
        List<Integer> odds = IntStream.range(0, 10).filter(f -> f % 2 != 0)
                .boxed().collect(Collectors.toList());

        DAG dag = new DAG();
        Vertex evensVertex = new Vertex("evens", () -> new ListProducer(evens, 4))
                .parallelism(1);

        Vertex oddsVertex = new Vertex("odds", () -> new ListProducer(odds, 4))
                .parallelism(1);


        Vertex processor = new Vertex("processor",
                () -> new SplittingMapper(o -> (int) o * (int) o, o -> (int) o * (int) o * (int) o))
                .parallelism(4);

        ListConsumer lhsConsumer = new ListConsumer();
        ListConsumer rhsConsumer = new ListConsumer();

        Vertex lhs = new Vertex("lhs", () -> lhsConsumer)
                .parallelism(1);

        Vertex rhs = new Vertex("rhs", () -> rhsConsumer)
                .parallelism(1);

        dag
                .addVertex(evensVertex)
                .addVertex(oddsVertex)
                .addVertex(processor)
                .addVertex(lhs)
                .addVertex(rhs)
                .addEdge(new Edge(evensVertex, 0, processor, 0))
                .addEdge(new Edge(oddsVertex, 0, processor, 1))
                .addEdge(new Edge(processor, 0, lhs, 0))
                .addEdge(new Edge(processor, 1, rhs, 0));

        Job job = jetEngine.newJob(dag);

        job.execute();

        System.out.println(lhsConsumer.getList());
        System.out.println(rhsConsumer.getList());
    }

    private static class SplittingMapper extends AbstractProcessor {

        private final Function<Object, Object> left;
        private final Function<Object, Object> right;

        public SplittingMapper(Function<Object, Object> left,
                               Function<Object, Object> right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean process(int ordinal, Object item) {
            if (ordinal == 0) {
                emit(0, left.apply(item));
            }
            else if (ordinal == 1) {
                emit(1, right.apply(item));
            }
            return true;
        }
    }
}
