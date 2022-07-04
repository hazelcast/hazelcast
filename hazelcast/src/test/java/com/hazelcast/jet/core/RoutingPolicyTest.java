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

import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.core.TestProcessors.CollectPerProcessorSink;
import com.hazelcast.jet.core.TestProcessors.ListsSourceP;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.TestUtil.executeAndPeel;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class RoutingPolicyTest extends SimpleTestInClusterSupport {

    private static final List<Integer> NUMBERS_LOW = IntStream.range(0, 4096).boxed().collect(toList());
    private static final List<Integer> NUMBERS_HIGH = IntStream.range(4096, 8192).boxed().collect(toList());

    private CollectPerProcessorSink consumerSup;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Before
    public void before() {
        TestProcessors.reset(1);
        consumerSup = new CollectPerProcessorSink();
    }

    @Test
    public void when_unicast() throws Throwable {
        DAG dag = new DAG();
        Vertex producer = producer(NUMBERS_LOW, NUMBERS_HIGH);
        Vertex consumer = consumer(2);

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
        Vertex consumer = consumer(4);

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
        Vertex consumer = consumer(2);

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
        Vertex consumer = consumer(2);

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
        Vertex consumer = consumer(4);

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

    @Test
    public void when_isolated_downstreamLowerThanUpstream() throws Throwable {
        DAG dag = new DAG();
        Vertex producer = producer(NUMBERS_LOW, NUMBERS_HIGH);
        Vertex consumer = consumer(1);

        dag.vertex(producer)
           .vertex(consumer)
           .edge(between(producer, consumer)
                   .isolated());

        execute(dag);

        List<Object> list = consumerSup.getListAt(0);
        assertEquals(setOf(NUMBERS_LOW, NUMBERS_HIGH), setOf(list));
    }

    @Test
    public void when_isolated_downstreamLowerThanUpstream_2() throws Throwable {

        final List<Integer> numbersHighest = IntStream.range(8192, 16384).boxed().collect(toList());

        DAG dag = new DAG();
        Vertex producer = producer(NUMBERS_LOW, NUMBERS_HIGH, numbersHighest);
        Vertex consumer = consumer(2);

        dag.vertex(producer)
           .vertex(consumer)
           .edge(between(producer, consumer)
                   .isolated());

        execute(dag);

        List<Object> list1 = consumerSup.getListAt(0);
        List<Object> list2 = consumerSup.getListAt(1);
        assertEquals(setOf(NUMBERS_LOW, numbersHighest), setOf(list1));
        assertEquals(setOf(NUMBERS_HIGH), setOf(list2));
    }

    @Test
    public void when_fanout() throws Throwable {
        DAG dag = new DAG();
        Vertex producer = producer(NUMBERS_LOW, NUMBERS_HIGH);
        Vertex consumer = consumer(2);

        dag.vertex(producer)
           .vertex(consumer)
           .edge(between(producer, consumer)
                   .fanout());

        execute(dag);

        List<Object> combined = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            combined.addAll(consumerSup.getListAt(i));
        }

        assertEquals(NUMBERS_LOW.size() + NUMBERS_HIGH.size(), combined.size());
        assertEquals(setOf(NUMBERS_LOW, NUMBERS_HIGH), setOf(combined));
    }

    private void execute(DAG dag) throws Throwable {
        executeAndPeel(instance().getJet().newJob(dag));
    }

    private Vertex consumer(int localParallelism) {
        return new Vertex("consumer", consumerSup).localParallelism(localParallelism);
    }

    private static void assertDisjoint(Collection<Object> items, Collection<Object> items2) {
        assertTrue("sets were not disjoint", Collections.disjoint(items, items2));
    }

    private static Vertex producer(List<?>... lists) {
        return new Vertex("producer", new ListsSourceP(lists))
                .localParallelism(lists.length);
    }

    @SafeVarargs
    private static <T> Set<T> setOf(Collection<T>... collections) {
        Set<T> set = new HashSet<>();
        int totalSize = 0;
        for (Collection<T> collection : collections) {
            set.addAll(collection);
            totalSize += collection.size();
        }
        assertEquals("there were some duplicates in the collections", totalSize, set.size());
        return set;
    }
}
