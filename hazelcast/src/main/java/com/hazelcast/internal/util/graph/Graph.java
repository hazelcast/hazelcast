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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptySet;

/**
 * A basic implementation for simple & undirected graphs.
 * "Simple graph" means that a vertex cannot have an edge to itself
 * and there can be at most one edge between two vertices.
 *
 * @param <V> Type of vertices
 */
public class Graph<V> {

    private Map<V, Set<V>> adjacencyMap = new HashMap<>();

    public void add(V v) {
        Objects.requireNonNull(v);
        adjacencyMap.putIfAbsent(v, new HashSet<>());
    }

    public void connect(V v1, V v2) {
        Objects.requireNonNull(v1);
        Objects.requireNonNull(v2);
        if (v1.equals(v2)) {
            return;
        }

        adjacencyMap.computeIfAbsent(v1, v -> new HashSet<>()).add(v2);
        adjacencyMap.computeIfAbsent(v2, v -> new HashSet<>()).add(v1);
    }

    public void disconnect(V v1, V v2) {
        Objects.requireNonNull(v1);
        Objects.requireNonNull(v2);
        if (v1.equals(v2)) {
            return;
        }

        if (adjacencyMap.getOrDefault(v1, emptySet()).remove(v2)) {
            adjacencyMap.get(v2).remove(v1);
        }
    }

    public Set<V> vertexSet() {
        return Collections.unmodifiableSet(adjacencyMap.keySet());
    }

    public boolean containsEdge(V v1, V v2) {
        Objects.requireNonNull(v1);
        Objects.requireNonNull(v2);
        return adjacencyMap.getOrDefault(v1, emptySet()).contains(v2);
    }

}
