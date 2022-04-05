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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.util.graph.BronKerboschCliqueFinder;
import com.hazelcast.internal.util.graph.Graph;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.spi.properties.ClusterProperty.HEARTBEAT_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.MAX_NO_HEARTBEAT_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.PARTIAL_MEMBER_DISCONNECTION_RESOLUTION_ALGORITHM_TIMEOUT_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.PARTIAL_MEMBER_DISCONNECTION_RESOLUTION_HEARTBEAT_COUNT;
import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Collects the disconnections that occur between the slave members in the
 * cluster. These disconnections occur because of network issues between slave
 * members and the master member do not encounter those network issues because
 * otherwise the master could have already kicked those members.
 * <p>
 * Once a set of disconnections are collected, the master member decides on
 * a minimum set of members to kick from the cluster so that the remaining
 * members do not have any network problem.
 * <p>
 * Used only by the master member.
 */
class PartialDisconnectionHandler {
    private final long detectionIntervalMs;
    private final long algorithmTimeoutNanos;
    private Map<MemberImpl, Set<MemberImpl>> disconnections = new HashMap<>();
    private long lastUpdated;

    PartialDisconnectionHandler(HazelcastProperties properties) {
        int partialDisconnectionResolutionHeartbeatCount = properties.getInteger(
                PARTIAL_MEMBER_DISCONNECTION_RESOLUTION_HEARTBEAT_COUNT);
        if (partialDisconnectionResolutionHeartbeatCount > 0) {
            long heartbeatIntervalSecs = properties.getInteger(HEARTBEAT_INTERVAL_SECONDS);
            long detectionIntervalSecs = partialDisconnectionResolutionHeartbeatCount * heartbeatIntervalSecs;
            long heartbeatTimeoutSecs = properties.getInteger(MAX_NO_HEARTBEAT_SECONDS);
            if (heartbeatTimeoutSecs < detectionIntervalSecs) {
                detectionIntervalSecs = heartbeatTimeoutSecs;
            }
            this.detectionIntervalMs = SECONDS.toMillis(detectionIntervalSecs);
        } else {
            this.detectionIntervalMs = Long.MAX_VALUE;
        }

        long algorithmTimeoutSecs = properties.getLong(PARTIAL_MEMBER_DISCONNECTION_RESOLUTION_ALGORITHM_TIMEOUT_SECONDS);
        this.algorithmTimeoutNanos = algorithmTimeoutSecs >= 1 ? SECONDS.toNanos(algorithmTimeoutSecs) : Long.MAX_VALUE;
    }

    /**
     * Updates the disconnected members set for the given member if the given
     * timestamp is greater than the highest observed timestamp.
     *
     * @return true if the internal disconnected members set is updated.
     */
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

    /**
     * Returns true if there is at least 1 disconnection reported and
     * {@code detectionIntervalMs} has passed after the last disconnection is
     * reported.
     */
    boolean shouldResolvePartialDisconnections(long timestamp) {
        return !disconnections.isEmpty() && timestamp - lastUpdated >= detectionIntervalMs;
    }

    /**
     * Receives a map of disconnections and returns a minimum set of members to
     * kick so that the remaining members are have no disconnections.
     * <p>
     * The key of the disconnection map is a member and the value is its
     * disconnection reports.
     *
     * @throws TimeoutException if the internally-used algorithm times out without
     *                          returning a result
     */
    Collection<MemberImpl> resolve(Map<MemberImpl, Set<MemberImpl>> disconnections) throws TimeoutException {
        Set<MemberImpl> members = new HashSet<>();
        disconnections.forEach((k, v) -> {
            members.add(k);
            members.addAll(v);
        });

        Graph<MemberImpl> connectivityGraph = buildConnectionGraph(members, disconnections);
        BronKerboschCliqueFinder<MemberImpl> cliqueFinder = createCliqueFinder(connectivityGraph);
        Collection<Set<MemberImpl>> maxCliques = cliqueFinder.computeMaxCliques();

        if (cliqueFinder.isTimeLimitReached()) {
            throw new TimeoutException("Partial disconnection resolution algorithm timed out! disconnectivity map: "
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
        return new BronKerboschCliqueFinder<>(graph, algorithmTimeoutNanos, NANOSECONDS);
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
