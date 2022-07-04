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

import com.google.common.collect.ImmutableSet;
import com.hazelcast.cluster.Address;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.core.TestProcessors.CollectPerProcessorSink;
import com.hazelcast.jet.core.TestProcessors.ListsSourceP;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.Edge.between;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
public class RoutingPolicyDistributedTest extends SimpleTestInClusterSupport {

    @SuppressWarnings("unchecked")
    private static final List<Integer>[] NUMBERS = new List[]{
            IntStream.range(0, 4096).boxed().collect(toList()),
            IntStream.range(4096, 8192).boxed().collect(toList()),
            IntStream.range(8192, 12288).boxed().collect(toList()),
            IntStream.range(12288, 16384).boxed().collect(toList())
    };

    private static Address address1;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private CollectPerProcessorSink consumerSup;

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
        address1 = instances()[1].getCluster().getLocalMember().getAddress();
    }

    @Before
    public void before() {
        TestProcessors.reset(1);
        consumerSup = new CollectPerProcessorSink();
    }

    @Test
    public void when_distributedToOne_partitioned() {
        DAG dag = new DAG();
        Vertex producer = producer(NUMBERS);
        Vertex consumer = consumer();

        dag.vertex(producer)
           .vertex(consumer)
           .edge(between(producer, consumer).distributeTo(address1).partitioned((Integer i) -> i % 271));

        instance().getJet().newJob(dag).join();

        for (int i = 0; i < consumerSup.getLists().size(); i++) {
            logger.info("size" + i + ": " + consumerSup.getListAt(i).size());
        }
        logger.info("size of union: " + consumerSup.getLists().stream().flatMap(List::stream).distinct().count());

        assertEquals("list count", 4, consumerSup.getLists().size());

        Set<Object> items0 = setOf(consumerSup.getListAt(0), consumerSup.getListAt(1));
        Set<Object> items1 = setOf(consumerSup.getListAt(2), consumerSup.getListAt(3));

        // all items should be sent to address1. However, the member at index 0 might be the address 1,
        // swap it in that case
        if (address1.equals(consumerSup.getMembers().get(0))) {
            Set<Object> itemsTmp = items0;
            items0 = items1;
            items1 = itemsTmp;
        }

        // now items0 are items sent to address0 and items1 are items sent to address1
        assertEquals("items on member0", emptySet(), items0);
        assertEquals("items on member1", setOf(NUMBERS), items1);
    }

    @Test
    public void when_distributedToOne_broadcast() {
        when_distributedToOne_notPartitioned(Edge::broadcast, "must be partitioned");
    }

    @Test
    public void when_distributedToOne_unicast() {
        when_distributedToOne_notPartitioned(Edge::unicast, "must be partitioned");
    }

    @Test
    public void when_distributedToOne_isolated() {
        when_distributedToOne_notPartitioned(Edge::isolated, "Isolated edges must be local");
    }

    @Test
    public void when_distributedToOne_fanout() {
        when_distributedToOne_notPartitioned(Edge::fanout, "must be partitioned");
    }

    private void when_distributedToOne_notPartitioned(Consumer<Edge> configureEdgeFn, String expectedError) {
        DAG dag = new DAG();
        Vertex producer = producer(NUMBERS);
        Vertex consumer = consumer();

        Edge edge = between(producer, consumer).distributeTo(address1);
        configureEdgeFn.accept(edge);

        dag.vertex(producer)
           .vertex(consumer)
           .edge(edge);

        exception.expectMessage(expectedError);
        instance().getJet().newJob(dag).join();
    }

    @Test
    public void when_distributedToOne_and_targetMemberMissing() throws Exception {
        DAG dag = new DAG();
        Vertex producer = producer(NUMBERS);
        Vertex consumer = consumer();

        dag.vertex(producer)
           .vertex(consumer)
           .edge(between(producer, consumer)
                   .distributeTo(new Address("1.2.3.4", 9999))
                   .allToOne("foo"));

        exception.expectMessage("The target member of an edge is not present in the cluster");
        instance().getJet().newJob(dag).join();
    }

    @Test
    public void when_local_fanout() {
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", new ListsSourceP(asList(1, 2), asList(3, 4))).localParallelism(1);
        Vertex consumer = new Vertex("consumer", consumerSup).localParallelism(2);

        dag.vertex(producer)
           .vertex(consumer)
           .edge(between(producer, consumer).fanout());

        instance().getJet().newJob(dag).join();

        assertEquals("items on member0-processor0", ImmutableSet.of(1), new HashSet<>(consumerSup.getListAt(0)));
        assertEquals("items on member0-processor1", ImmutableSet.of(2), new HashSet<>(consumerSup.getListAt(1)));
        assertEquals("items on member1-processor0", ImmutableSet.of(3), new HashSet<>(consumerSup.getListAt(2)));
        assertEquals("items on member1-processor1", ImmutableSet.of(4), new HashSet<>(consumerSup.getListAt(3)));
    }

    @Test
    public void when_distributed_fanout() {
        DAG dag = new DAG();
        Vertex producer = new Vertex("producer", new ListsSourceP(asList(1, 2), asList(3, 4))).localParallelism(1);
        Vertex consumer = new Vertex("consumer", consumerSup).localParallelism(2);

        dag.vertex(producer)
           .vertex(consumer)
           .edge(between(producer, consumer).distributed().fanout());

        instance().getJet().newJob(dag).join();

        assertEquals("items on member0-processor0", ImmutableSet.of(1, 3), new HashSet<>(consumerSup.getListAt(0)));
        assertEquals("items on member0-processor1", ImmutableSet.of(2, 4), new HashSet<>(consumerSup.getListAt(1)));
        assertEquals("items on member1-processor0", ImmutableSet.of(1, 3), new HashSet<>(consumerSup.getListAt(2)));
        assertEquals("items on member1-processor1", ImmutableSet.of(2, 4), new HashSet<>(consumerSup.getListAt(3)));
    }

    private Vertex consumer() {
        return new Vertex("consumer", consumerSup)
                .localParallelism(2);
    }

    private static Vertex producer(List<?>... lists) {
        assertEquals(0, lists.length % instances().length);
        return new Vertex("producer", new ListsSourceP(lists))
                .localParallelism(lists.length / instances().length);
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
