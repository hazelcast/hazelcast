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

package com.hazelcast.test.mocknetwork;

import com.hazelcast.spi.properties.HazelcastProperty;

import static com.hazelcast.spi.properties.ClusterProperty.TCP_JOIN_PORT_TRY_COUNT;

/**
 * The cluster properties that define the behavior of the mock networking of
 * Hazelcast's test network infrastructure.
 */
public final class MockClusterProperty {

    /**
     * Test config property to set if {@link MockJoiner} is desired to prevent
     * attempting to join members that are expected to be members of different
     * clusters. If the two members are member of the same cluster is determined
     * by checking the ports of the members.
     *
     * @see MockJoiner
     */
    public static final HazelcastProperty MOCK_JOIN_SHOULD_ISOLATE_CLUSTERS
            = new HazelcastProperty("hazelcast.mock.join.should.isolate.clusters", false);

    /**
     * The number of incremental ports, starting with port number defined in
     * network configuration, that will be used to connect to a host which is
     * defined without a port in the TCP-IP member list while a node is searching
     * for a cluster.
     */
    public static final HazelcastProperty MOCK_JOIN_PORT_TRY_COUNT
            = new HazelcastProperty("hazelcast.mock.join.port.try.count",
            TCP_JOIN_PORT_TRY_COUNT.getDefaultValue());

    private MockClusterProperty() {
    }
}
