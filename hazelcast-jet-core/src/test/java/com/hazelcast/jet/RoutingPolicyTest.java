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

package com.hazelcast.jet;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.TestUtil.executeAndPeel;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class RoutingPolicyTest extends JetTestSupport {

    private static final List<Integer> NUMBERS_LOW = IntStream.range(0, 4096).boxed().collect(toList());
    private static final List<Integer> NUMBERS_HIGH = IntStream.range(4096, 8192).boxed().collect(toList());

    private JetInstance instance;
    private JetTestInstanceFactory factory;
    private ListConsumerSup consumerSup;

    @Before
    public void setupEngine() {
        factory = new JetTestInstanceFactory();
        instance = factory.newMember();
        consumerSup = new ListConsumerSup();
    }

    @After
    public void shutdown() {
        factory.shutdownAll();
    }

    @Test
    public void when_unicast() throws Throwable {
        DAG dag = new DAG();
        Vertex producer = producer(NUMBERS_LOW, NUMBERS_HIGH);
        Vertex consumer = consumer(consumerSup, 2);

        dag.vertex(producer)
           .vertex(consumer)
           .edge(between(producer, consumer));

        execute(dag);

        List<Object> combined = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            combined.addAll(consumerSup.getListAt(i));
        }

        assertEquals(NUMBERS_LOW.size() + NUMBERS_HIGH.size(), combined.size());

        assertEquals(setOf(NUMBERS_LOW, NUMBERS_HIGH), setOf(combined));

        System.out.println(consumerSup.getListAt(0));
        System.out.println(consumerSup.getListAt(1));
    }

    @Test
    public void when_broadcast() throws Throwable {
        DAG dag = new DAG();
        Vertex producer = producer(NUMBERS_LOW, NUMBERS_HIGH);
        Vertex consumer = consumer(consumerSup, 4);

        dag.vertex(producer)
           .vertex(consumer)
           .edge(between(producer, consumer).broadcast());

        execute(dag);

        Set<Integer> expected = setOf(NUMBERS_LOW, NUMBERS_HIGH);
        for (int i = 0; i < 4; i++) {
            assertEquals(expected, setOf(consumerSup.getListAt(i)));
        }
    }

    @Test
    public void when_partitioned() throws Throwable {
        DAG dag = new DAG();
        Vertex producer = producer(NUMBERS_LOW, NUMBERS_LOW, NUMBERS_HIGH, NUMBERS_HIGH);
        Vertex consumer = consumer(consumerSup, 2);

        dag.vertex(producer)
           .vertex(consumer)
           .edge(between(producer, consumer)
                   .partitioned(wholeItem()));

        execute(dag);

        assertDisjoint(consumerSup.getListAt(0), consumerSup.getListAt(1));
    }

    @Test
    public void when_isolated_downstreamEqualsUpstream() throws Throwable {
        DAG dag = new DAG();
        Vertex producer = producer(NUMBERS_LOW, NUMBERS_HIGH);
        Vertex consumer = consumer(consumerSup, 2);

        dag.vertex(producer)
           .vertex(consumer)
           .edge(between(producer, consumer)
                   .isolated());

        execute(dag);

        assertEquals(NUMBERS_LOW, consumerSup.getListAt(0));
        assertEquals(NUMBERS_HIGH, consumerSup.getListAt(1));
    }

    @Test
    public void when_isolated_downstreamGreaterThanUpstream() throws Throwable {
        DAG dag = new DAG();
        Vertex producer = producer(NUMBERS_LOW, NUMBERS_HIGH);
        Vertex consumer = consumer(consumerSup, 4);

        dag.vertex(producer)
           .vertex(consumer)
           .edge(between(producer, consumer)
                   .isolated());

        execute(dag);

        List<Object> list0 = consumerSup.getListAt(0);
        List<Object> list1 = consumerSup.getListAt(1);
        List<Object> list2 = consumerSup.getListAt(2);
        List<Object> list3 = consumerSup.getListAt(3);

        assertEquals(setOf(NUMBERS_LOW), setOf(list0, list2));
        assertEquals(setOf(NUMBERS_HIGH), setOf(list1, list3));
    }

    private void execute(DAG dag) throws Throwable {
        executeAndPeel(instance.newJob(dag));
    }

    private static Vertex consumer(ListConsumerSup consumerSup, int count) {
        return new Vertex("consumer", consumerSup).localParallelism(count);
    }

    private static void assertDisjoint(Collection<Object> items, Collection<Object> items2) {
        assertTrue("sets were not disjoint", Collections.disjoint(items, items2));
    }

    private static Vertex producer(List<?>... lists) {
        return new Vertex("producer", new ListProducerSup(4, lists))
                .localParallelism(lists.length);
    }

    @SafeVarargs
    private static <T> Set<T> setOf(Collection<T>... collections) {
        Set<T> set = new HashSet<>();
        for (Collection<T> collection : collections) {
            set.addAll(collection);
        }
        return set;
    }

    private static class ListProducerSup implements ProcessorSupplier {

        private final List<?>[] lists;
        private final int batchSize;

        ListProducerSup(int batchSize, List<?>... lists) {
            this.batchSize = batchSize;
            this.lists = lists;
        }

        @Override
        public void init(@Nonnull Context context) {
            if (context.localParallelism() != lists.length) {
                throw new IllegalArgumentException("Supplied list count does not equal local parallelism");
            }
        }

        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            return Arrays.stream(lists).map(ListSource::new).collect(
                    Collectors.toList());
        }
    }

    private static class ListConsumerSup implements ProcessorSupplier {

        private List<Processor> processors;

        @Override
        public void init(@Nonnull Context context) {
            processors = Stream.generate(ListSink::new).limit(context.localParallelism()).collect(toList());
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            assertEquals(processors.size(), count);
            return processors;
        }

        List<Object> getListAt(int i) {
            return ((ListSink) processors.get(i)).getList();
        }
    }
}
