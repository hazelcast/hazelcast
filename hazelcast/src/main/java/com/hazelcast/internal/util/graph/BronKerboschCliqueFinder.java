/*
*  Original work Copyright (c) 2005-2021, by Ewgenij Proschak and Contributors.
 * Modified work Copyright (c) 2021, Hazelcast, Inc. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class BronKerboschCliqueFinder<V> {

    private final Graph<V> graph;
    private final long nanos;
    private boolean timeLimitReached;
    private List<Set<V>> maximumCliques;

    /**
     * Constructor
     *
     * @param graph   the input graph; must be simple
     * @param timeout the maximum time to wait, if zero no timeout
     * @param unit    the time unit of the timeout argument
     */
    public BronKerboschCliqueFinder(Graph<V> graph, long timeout, TimeUnit unit) {
        this.graph = requireNonNull(graph, "Graph cannot be null");
        if (timeout < 1L) {
            throw new IllegalArgumentException("Invalid timeout, must be positive!");
        }
        this.nanos = unit.toNanos(timeout);
    }

    /**
     * Constructs a new clique finder.
     *
     * @param graph the input graph; must be "simple".
     */
    public BronKerboschCliqueFinder(Graph<V> graph) {
        this(graph, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    /**
     * Computes and returns the maximum cliques of the given graph.
     *
     * @return the maximum cliques of the given graph.
     */
    public Collection<Set<V>> computeMaxCliques() {
        lazyRun();
        return maximumCliques;
    }

    /**
     * Check the computation has stopped due to a time limit or due to computing all maximal
     * cliques.
     *
     * @return true if the computation has stopped due to a time limit, false otherwise
     */
    public boolean isTimeLimitReached() {
        return timeLimitReached;
    }

    /**
     * Lazily execute the enumeration algorithm.
     */
    private void lazyRun() {
        if (maximumCliques != null) {
            return;
        }

        maximumCliques = new ArrayList<>();

        long nanosTimeLimit;
        try {
            nanosTimeLimit = Math.addExact(System.nanoTime(), nanos);
        } catch (ArithmeticException e) {
            nanosTimeLimit = Long.MAX_VALUE;
        }

        findCliques(new HashSet<>(), new HashSet<>(graph.vertexSet()), new HashSet<>(), nanosTimeLimit);
    }

    private void findCliques(Collection<V> potentialClique, Collection<V> candidates, Collection<V> alreadyFound,
                             long nanosTimeLimit) {
        /*
         * Termination condition: check if any already found node is connected to all candidate
         * nodes.
         */
        for (V v : alreadyFound) {
            if (candidates.stream().allMatch(c -> graph.containsEdge(v, c))) {
                return;
            }
        }

        Iterator<V> it = candidates.iterator();
        while (it.hasNext()) {
            if (nanosTimeLimit - System.nanoTime() < 0) {
                timeLimitReached = true;
                return;
            }

            V candidate = it.next();
            it.remove();

            // move candidate node to potentialClique
            potentialClique.add(candidate);

            // create newCandidates by removing nodes in candidates
            // not connected to candidate node
            Collection<V> newCandidates = populate(candidates, candidate);

            // create newAlreadyFound by removing nodes in alreadyFound
            // not connected to candidate node
            Collection<V> newAlreadyFound = populate(alreadyFound, candidate);

            // if newCandidates and newAlreadyFound are empty
            if (newCandidates.isEmpty() && newAlreadyFound.isEmpty()) {
                // potential clique is maximum clique
                addMaxClique(potentialClique);
            } else {
                // recursive call
                findCliques(potentialClique, newCandidates, newAlreadyFound, nanosTimeLimit);
            }

            // move candidate node from potentialClique to alreadyFound
            alreadyFound.add(candidate);
            potentialClique.remove(candidate);
        }
    }

    private Collection<V> populate(Collection<V> candidates, V candidate) {
        Collection<V> newCandidates = new HashSet<>();
        for (V newCandidate : candidates) {
            if (graph.containsEdge(candidate, newCandidate)) {
                newCandidates.add(newCandidate);
            }
        }

        return newCandidates;
    }

    private void addMaxClique(Collection<V> potentialClique) {
        if (maximumCliques.isEmpty() || potentialClique.size() == maximumCliques.get(0).size()) {
            maximumCliques.add(new HashSet<>(potentialClique));
        } else if (potentialClique.size() > maximumCliques.get(0).size()) {
            maximumCliques.clear();
            maximumCliques.add(new HashSet<>(potentialClique));
        }
    }

}
