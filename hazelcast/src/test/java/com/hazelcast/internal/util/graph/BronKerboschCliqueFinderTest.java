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

package com.hazelcast.internal.util.graph;

import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class BronKerboschCliqueFinderTest {

    @Test
    public void test2DisconnectedVerticesIn4vertexGraph() {
        test2DisconnectedVertices(4);
    }

    @Test
    public void test2DisconnectedVerticesIn50vertexGraph() {
        test2DisconnectedVertices(50);
    }

    @Test
    public void test2DisconnectedVerticesIn100vertexGraph() {
        test2DisconnectedVertices(100);
    }

    @Test
    public void test2DisconnectedVerticesIn250vertexGraph() {
        test2DisconnectedVertices(250);
    }

    private void test2DisconnectedVertices(int vertexCount) {
        List<String> vertices = IntStream.range(0, vertexCount).mapToObj(i -> "n" + i).collect(toList());
        Graph<String> graph = populateFullyConnectedGraph(vertices);

        graph.disconnect("n0", "n1");


        Collection<Set<String>> maxCliques = computeMaxCliques(graph);


        Set<Set<String>> expectedCliques = new HashSet<>();
        Set<String> expectedClique1 = new HashSet<>(vertices.subList(2, vertexCount));
        expectedClique1.add("n0");
        Set<String> expectedClique2 = new HashSet<>(vertices.subList(2, vertexCount));
        expectedClique2.add("n1");
        expectedCliques.add(expectedClique1);
        expectedCliques.add(expectedClique2);

        assertEquals(expectedCliques, new HashSet<>(maxCliques));
    }

    private Collection<Set<String>> computeMaxCliques(Graph<String> graph) {
        return new BronKerboschCliqueFinder<>(graph).computeMaxCliques();
    }

    @Test
    public void testSplitInto4VertexLeftCliqueAnd4VertexRightClique() {
        testFullSplitInto2Cliques(8, 4);
    }

    @Test
    public void testSplitInto8VertexLeftCliqueAnd5vertexRightClique() {
        testFullSplitInto2Cliques(8, 5);
    }

    @Test
    public void testSplitInto25VertexLeftCliqueAnd25VertexRightClique() {
        testFullSplitInto2Cliques(50, 25);
    }

    @Test
    public void testSplitInto15VertexLeftCliqueAnd35VertexRightClique() {
        testFullSplitInto2Cliques(50, 15);
    }

    @Test
    public void testSplitInto50VertexLeftCliqueAnd50VertexRightClique() {
        testFullSplitInto2Cliques(100, 50);
    }

    @Test
    public void testSplitInto75VertexLeftCliqueAnd25VertexRightClique() {
        testFullSplitInto2Cliques(100, 75);
    }

    @Test
    public void testSplitInto125VertexLeftCliqueAnd125VertexRightClique() {
        testFullSplitInto2Cliques(250, 125);
    }

    @Test
    public void testSplitInto100VertexLeftCliqueAnd150VertexRightClique() {
        testFullSplitInto2Cliques(250, 100);
    }

    private void testFullSplitInto2Cliques(int vertexCount, int leftCliqueSize) {
        List<String> vertices = IntStream.range(0, vertexCount).mapToObj(i -> "n" + i).collect(toList());
        Graph<String> graph = populateFullyConnectedGraph(vertices);

        List<String> left = vertices.subList(0, leftCliqueSize);
        List<String> right = vertices.subList(leftCliqueSize, vertices.size());

        for (String v1 : left) {
            for (String v2 : right) {
                graph.disconnect(v1, v2);
            }
        }


        Collection<Set<String>> maxCliques = computeMaxCliques(graph);


        Set<Set<String>> expectedCliques = new HashSet<>();
        if (left.size() == right.size()) {
            expectedCliques.add(new HashSet<>(left));
            expectedCliques.add(new HashSet<>(right));
        } else if (left.size() < right.size()) {
            expectedCliques.add(new HashSet<>(right));
        } else {
            expectedCliques.add(new HashSet<>(left));
        }


        assertEquals(expectedCliques, new HashSet<>(maxCliques));
    }

    @Test
    public void test3VerticesDisconnectFrom2VerticesIn10VertexGraph() {
        testTwoDisconnectedSubgraphs(10, 3, 2);
    }

    @Test
    public void test3VerticesDisconnectFrom3VerticesIn10VertexGraph() {
        testTwoDisconnectedSubgraphs(10, 3, 3);
    }

    @Test
    public void test10VerticesDisconnectFrom10VerticesIn50VertexGraph() {
        testTwoDisconnectedSubgraphs(50, 10, 10);
    }

    @Test
    public void test15VerticesDisconnectFrom10VerticesIn50VertexGraph() {
        testTwoDisconnectedSubgraphs(50, 15, 10);
    }

    @Test
    public void test20VerticesDisconnectFrom20VerticesIn100VertexGraph() {
        testTwoDisconnectedSubgraphs(100, 20, 20);
    }

    @Test
    public void test30VerticesDisconnectFrom20VerticesIn100VertexGraph() {
        testTwoDisconnectedSubgraphs(100, 30, 20);
    }

    @Test
    public void test50VerticesDisconnectFrom50VerticesIn250VertexGraph() {
        testTwoDisconnectedSubgraphs(250, 50, 50);
    }

    @Test
    public void test100VerticesDisconnectFrom50VerticesIn250VertexGraph() {
        testTwoDisconnectedSubgraphs(250, 100, 50);
    }

    private void testTwoDisconnectedSubgraphs(int vertexCount, int firstGroupSize, int secondGroupSize) {
        List<String> vertices = IntStream.range(0, vertexCount).mapToObj(i -> "n" + i).collect(toList());
        Graph<String> graph = populateFullyConnectedGraph(vertices);

        for (int i = 0; i < firstGroupSize; i++) {
            for (int j = firstGroupSize; j < firstGroupSize + secondGroupSize; j++) {
                graph.disconnect("n" + i, "n" + j);
            }
        }

        Collection<Set<String>> maxCliques = computeMaxCliques(graph);


        Set<Set<String>> expectedCliques = new HashSet<>();
        if (firstGroupSize == secondGroupSize) {
            Set<String> expectedClique1 = new HashSet<>(vertices.subList(0, firstGroupSize));
            Set<String> expectedClique2 = new HashSet<>(vertices.subList(firstGroupSize, firstGroupSize + secondGroupSize));
            expectedClique1.addAll(vertices.subList(firstGroupSize + secondGroupSize, vertices.size()));
            expectedClique2.addAll(vertices.subList(firstGroupSize + secondGroupSize, vertices.size()));
            expectedCliques.add(expectedClique1);
            expectedCliques.add(expectedClique2);
        } else if (firstGroupSize < secondGroupSize) {
            Set<String> expectedClique = new HashSet<>(vertices.subList(firstGroupSize, firstGroupSize + secondGroupSize));
            expectedClique.addAll(vertices.subList(firstGroupSize + secondGroupSize, vertices.size()));
            expectedCliques.add(expectedClique);
        } else {
            Set<String> expectedClique = new HashSet<>(vertices.subList(0, firstGroupSize));
            expectedClique.addAll(vertices.subList(firstGroupSize + secondGroupSize, vertices.size()));
            expectedCliques.add(expectedClique);
        }

        assertEquals(expectedCliques, new HashSet<>(maxCliques));
    }

    @Test
    public void test6DisconnectedSubgraphsInLargerGraph() {
        List<List<String>> groups = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            int j = i;
            int vertexCount = i + 10;
            List<String> vertices = IntStream.range(0, vertexCount).mapToObj(v -> j + "_" + v).collect(toList());
            groups.add(vertices);
        }

        Graph<String> graph = populateFullyConnectedGraph(groups.stream().flatMap(Collection::stream).collect(toList()));

        for (BiTuple<Integer, Integer> t : Arrays.asList(BiTuple.of(0, 9), BiTuple.of(1, 8), BiTuple.of(2, 7), BiTuple.of(3, 6),
                BiTuple.of(4, 5))) {
            for (String v1 : groups.get(t.element1)) {
                for (String v2 : groups.get(t.element2)) {
                    graph.disconnect(v1, v2);
                }
            }
        }


        Collection<Set<String>> maxCliques = new BronKerboschCliqueFinder<>(graph, 60, TimeUnit.SECONDS).computeMaxCliques();


        assumeFalse(maxCliques.isEmpty());

        Set<String> expectedClique = new HashSet<>();
        for (int i = groups.size() / 2; i < groups.size(); i++) {
            expectedClique.addAll(groups.get(i));
        }

        assertEquals(1, maxCliques.size());
        assertEquals(expectedClique, maxCliques.iterator().next());
    }

    @Test
    public void test6DisconnectedSubgraphsOfWholeGraph() {
        List<List<String>> groups = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            int j = i;
            int vertexCount = i + 10;
            List<String> vertices = IntStream.range(0, vertexCount).mapToObj(v -> j + "_" + v).collect(toList());
            groups.add(vertices);
        }

        Graph<String> graph = populateFullyConnectedGraph(groups.stream().flatMap(Collection::stream).collect(toList()));

        List<Integer> groupIndices = new ArrayList<>();
        while (groupIndices.size() < 6) {
            int rackIndex = RandomPicker.getInt(groups.size());
            if (!groupIndices.contains(rackIndex)) {
                groupIndices.add(rackIndex);
            }
        }

        groupIndices.sort(Comparator.comparingInt((ToIntFunction<Integer>) i -> groups.get(i).size()).reversed());

        for (String v1 : groups.get(groupIndices.get(0))) {
            for (String v2 : groups.get(groupIndices.get(5))) {
                graph.disconnect(v1, v2);
            }
        }

        for (String v1 : groups.get(groupIndices.get(1))) {
            for (String v2 : groups.get(groupIndices.get(4))) {
                graph.disconnect(v1, v2);
            }
        }

        for (String v1 : groups.get(groupIndices.get(2))) {
            for (String v2 : groups.get(groupIndices.get(3))) {
                graph.disconnect(v1, v2);
            }
        }

        Collection<Set<String>> maxCliques = new BronKerboschCliqueFinder<>(graph, 60, TimeUnit.SECONDS).computeMaxCliques();


        assumeFalse(maxCliques.isEmpty());

        Set<String> expectedClique = new HashSet<>();
        for (int i = 0; i < groups.size(); i++) {
            if (i != groupIndices.get(3) && i != groupIndices.get(4) && i != groupIndices.get(5)) {
                expectedClique.addAll(groups.get(i));
            }
        }

        assertEquals(1, maxCliques.size());
        assertEquals(expectedClique, maxCliques.iterator().next());
    }

    private Graph<String> populateFullyConnectedGraph(List<String> vertices) {
        Graph<String> graph = new Graph<>();

        for (String v1 : vertices) {
            for (String v2 : vertices) {
                graph.connect(v1, v2);
            }
        }

        return graph;
    }

}
