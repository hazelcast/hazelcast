/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.util.graph.BronKerboschCliqueFinder;
import com.hazelcast.internal.util.graph.Graph;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class PartialDisconnectionHandler {
    private final long detectionIntervalMs;
    private final long algorithmTimeoutMs;
    private Map<MemberImpl, Set<MemberImpl>> disconnections = new HashMap<>();
    private long lastUpdated;

    PartialDisconnectionHandler(long detectionIntervalMs, long algorithmTimeoutMs) {
        this.detectionIntervalMs = detectionIntervalMs;
        this.algorithmTimeoutMs = algorithmTimeoutMs;
    }

    boolean update(MemberImpl member, long timestamp, Collection<MemberImpl> disconnectedMembers) {
        if (timestamp < lastUpdated) {
            return false;
        }

        Set<MemberImpl> currentDisconnectedMembers =  disconnections.get(member);
        if (currentDisconnectedMembers == null) {
            if (disconnectedMembers.isEmpty()) {
                return false;
            }

            currentDisconnectedMembers = new HashSet<>();
            disconnections.put(member, currentDisconnectedMembers);
        }

        boolean updated = false;

        for (MemberImpl disconnectedMember : disconnectedMembers) {
            if (currentDisconnectedMembers.add(disconnectedMember)
                    && !disconnections.getOrDefault(disconnectedMember, emptySet()).contains(member)) {
                lastUpdated = timestamp;
                updated = true;
            }
        }

        if (currentDisconnectedMembers.retainAll(disconnectedMembers)) {
            lastUpdated = timestamp;
            updated = true;
        }

        if (currentDisconnectedMembers.isEmpty()) {
            disconnections.remove(member);
        }

        return updated;
    }

    boolean shouldResolvePartialDisconnections(long timestamp) {
        return !disconnections.isEmpty() && timestamp - lastUpdated >= detectionIntervalMs;
    }

    Collection<MemberImpl> resolve(Map<MemberImpl, Set<MemberImpl>> disconnections) {
        Set<MemberImpl> members = new HashSet<>();
        disconnections.forEach((k, v) -> {
            members.add(k);
            members.addAll(v);
        });

        Graph<MemberImpl> connectivityGraph = buildConnectionGraph(members, disconnections);
        BronKerboschCliqueFinder<MemberImpl> cliqueFinder = createCliqueFinder(connectivityGraph);
        Collection<Set<MemberImpl>> maxCliques = cliqueFinder.computeMaxCliques();

        if (cliqueFinder.isTimeLimitReached()) {
            throw new IllegalStateException("Partial disconnection resolution algorithm timed out! disconnectivity map: "
                    + disconnections);
        } else if (maxCliques.isEmpty()) {
            throw new IllegalStateException("Partial disconnection resolution algorithm returned no result! "
                    + "disconnectivity map: " + disconnections);
        }

        Collection<MemberImpl> membersToRemove = new HashSet<>(members);
        membersToRemove.removeAll(maxCliques.iterator().next());

        return membersToRemove;
    }

    private Graph<MemberImpl> buildConnectionGraph(Set<MemberImpl> members,
                                                   Map<MemberImpl, Set<MemberImpl>> disconnections) {
        Graph<MemberImpl> graph = new Graph<>();
        members.forEach(graph::add);

        for (MemberImpl member1 : members) {
            for (MemberImpl member2 : members) {
                if (!isDisconnected(disconnections, member1, member2)) {
                    graph.connect(member1, member2);
                }
            }
        }

        return graph;
    }

    private boolean isDisconnected(Map<MemberImpl, Set<MemberImpl>> disconnections, MemberImpl member1, MemberImpl member2) {
        return disconnections.getOrDefault(member1, emptySet()).contains(member2)
                || disconnections.getOrDefault(member2, emptySet()).contains(member1);
    }

    private BronKerboschCliqueFinder<MemberImpl> createCliqueFinder(Graph<MemberImpl> graph) {
        return new BronKerboschCliqueFinder<>(graph, algorithmTimeoutMs, MILLISECONDS);
    }

    void removeMember(MemberImpl member) {
        disconnections.remove(member);
        disconnections.values().forEach(members -> members.remove(member));
    }

    Map<MemberImpl, Set<MemberImpl>> reset() {
        Map<MemberImpl, Set<MemberImpl>> disconnections = this.disconnections;
        this.disconnections = new HashMap<>();
        return disconnections;
    }

    Map<MemberImpl, Set<MemberImpl>> getDisconnections() {
        return disconnections;
    }

}
