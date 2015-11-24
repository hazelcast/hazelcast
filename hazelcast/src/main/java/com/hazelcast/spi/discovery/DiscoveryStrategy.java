/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.annotation.Beta;

/**
 * The <tt>DiscoveryStrategy</tt> itself is the actual implementation to discover
 * nodes based on whatever environment is used to run the Hazelcast cloud. The
 * internal implementation is completely vendor specific and might use external Java
 * libraries to query APIs or send requests to a external REST service.
 * <p/>
 * <tt>DiscoveryStrategies</tt> are also free to define any kind of properties that might
 * be necessary to discover eligible nodes based on configured attributes, tags or any
 * other kind of metadata.
 * <p/>
 * Using the simple lifecycle management strategies like multicast discovery is able to
 * register and destroy sockets based on Hazelcast’s lifecycle. Deactivated services will
 * also never be started.
 *
 * @since 3.6
 */
@Beta
public interface DiscoveryStrategy {

    /**
     * The <tt>start</tt> method is used to initialize internal state and perform any kind of
     * startup procedure like multicast socket creation. The behavior of this method might
     * change based on the {@link DiscoveryNode} instance passed to the {@link DiscoveryStrategyFactory}.
     */
    void start();

    /**
     * Returns a set of all discovered nodes based on the defined properties that were used
     * to create the <tt>DiscoveryStrategy</tt> instance.
     *
     * @return a set of all discovered nodes
     */
    Iterable<DiscoveryNode> discoverNodes();

    /**
     * The <tt>stop</tt> method is used to stop internal services, sockets or to destroy any
     * kind of internal state.
     */
    void destroy();
}
