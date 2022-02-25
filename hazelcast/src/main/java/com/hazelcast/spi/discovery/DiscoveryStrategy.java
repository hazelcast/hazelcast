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

package com.hazelcast.spi.discovery;

import com.hazelcast.cluster.Member;
import com.hazelcast.spi.partitiongroup.PartitionGroupStrategy;

import java.util.Collection;
import java.util.Map;

/**
 * The <code>DiscoveryStrategy</code> itself is the actual implementation to discover
 * nodes based on whatever environment is used to run the Hazelcast cloud. The
 * internal implementation is completely vendor specific and might use external Java
 * libraries to query APIs or send requests to a external REST service.
 * <p>
 * <code>DiscoveryStrategies</code> are also free to define any kind of properties that might
 * be necessary to discover eligible nodes based on configured attributes, tags or any
 * other kind of metadata.
 * <p>
 * Using the simple lifecycle management strategies like multicast discovery is able to
 * register and destroy sockets based on Hazelcast’s lifecycle. Deactivated services will
 * also never be started.
 * <p>
 * <b>Attention:</b> Instead of implementing this interface directly, implementors should consider
 * using the abstract class {@link AbstractDiscoveryStrategy} instead for easier upgradeability
 * while this interface evolves.
 *
 * @since 3.6
 */
public interface DiscoveryStrategy {

    /**
     * The <code>start</code> method is used to initialize internal state and perform any kind of
     * startup procedure like multicast socket creation. The behavior of this method might
     * change based on the {@link DiscoveryNode} instance passed to the {@link DiscoveryStrategyFactory}.
     */
    void start();

    /**
     * Returns a set of all discovered nodes based on the defined properties that were used
     * to create the <code>DiscoveryStrategy</code> instance.
     *
     * @return a set of all discovered nodes
     */
    Iterable<DiscoveryNode> discoverNodes();

    /**
     * The <code>stop</code> method is used to stop internal services, sockets or to destroy any
     * kind of internal state.
     */
    void destroy();

    /**
     * Returns a custom implementation of a {@link PartitionGroupStrategy} to override
     * default behavior of zone aware backup strategies {@link com.hazelcast.spi.partitiongroup.PartitionGroupMetaData}
     * or to provide a specific behavior in case the discovery environment does not provide
     * information about the infrastructure to be used for automatic configuration.
     * @param allMembers Current state of Cluster data members, excluding lite members
     * @return a custom implementation of a <code>PartitionGroupStrategy</code> otherwise <code>null</code>
     * in case of the default implementation is to be used
     * @since 4.2.1
     */
    default PartitionGroupStrategy getPartitionGroupStrategy(Collection<? extends Member> allMembers) {
        return null;
    }

    /**
     * @deprecated - use the above method that takes allMember arguments
     * Returns a custom implementation of a {@link PartitionGroupStrategy} to override
     * default behavior of zone aware backup strategies {@link com.hazelcast.spi.partitiongroup.PartitionGroupMetaData}
     * or to provide a specific behavior in case the discovery environment does not provide
     * information about the infrastructure to be used for automatic configuration.
     *
     * @return a custom implementation of a <code>PartitionGroupStrategy</code> otherwise <code>null</code>
     * in case of the default implementation is to be used
     * @since 3.7
     */
    @Deprecated
    default PartitionGroupStrategy getPartitionGroupStrategy() {
        return null;
    }

    /**
     * Returns a map with discovered metadata provided by the runtime environment. Those information
     * may include, but are not limited, to location information like datacenter, node name
     * or additional tags to be used for custom purpose.
     * <p>
     * Information discovered from this method are shaded into the {@link Member}s
     * attributes. Existing attributes will not be overridden, that way local attribute configuration
     * overrides provided metadata.
     * <p>
     * The default implementation provides an empty map with no further metadata configured. Returning
     * <code>null</code> is not permitted and will most probably result in an {@link NullPointerException}
     * inside the cluster system.
     *
     * @return a map of discovered metadata as provided by the runtime environment
     * @since 3.7
     */
    Map<String, String> discoverLocalMetadata();
}
