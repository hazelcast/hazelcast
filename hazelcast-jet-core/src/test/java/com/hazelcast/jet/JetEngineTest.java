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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.impl.connector.IMapReader;
import com.hazelcast.jet.impl.connector.IMapWriter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.TestUtil.executeAndPeel;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class JetEngineTest extends JetTestSupport {

    private JetTestInstanceFactory factory;

    @After
    public void shutdownFactory() {
        factory.shutdownAll();
    }

    @Before
    public void setupEngine() {
        factory = new JetTestInstanceFactory();
    }

    @Test
    public void testExecuteFromClient() throws Throwable {
        factory.newMember();
        JetInstance client = factory.newClient();
        producerConsumerTest(client);
    }

    @Test
    public void testExecuteFromMember() throws Throwable {
        producerConsumerTest(factory.newMember());
    }


    @Test
    public void test() throws Throwable {
        JetInstance instance = factory.newMember();
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

        executeAndPeel(instance.newJob(dag));

        System.out.println(lhsConsumer.getList());
        System.out.println(rhsConsumer.getList());
    }


    private void producerConsumerTest(JetInstance instance) throws Throwable {
        IMap<Object, Object> map = instance.getMap("numbers");
        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }

        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", IMapReader.supplier("numbers"))
                .parallelism(1);

        Vertex consumer = new Vertex("consumer", IMapWriter.supplier("consumer"))
                .parallelism(1);

        dag.addVertex(producer)
           .addVertex(consumer)
           .addEdge(new Edge(producer, consumer));

        executeAndPeel(instance.newJob(dag));

        IMap<Object, Object> consumerMap = instance.getMap("consumer");
        assertEquals(map.entrySet(), consumerMap.entrySet());
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
            } else if (ordinal == 1) {
                emit(1, right.apply(item));
            }
            return true;
        }
    }
}
