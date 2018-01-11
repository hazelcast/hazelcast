/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.discovery.integration;

import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.NodeFilter;

import java.util.Map;

/**
 * The <tt>DiscoveryService</tt> interface defines the basic entry point
 * into the Discovery SPI implementation. If not overridden explicitly the Hazelcast
 * internal {@link com.hazelcast.spi.discovery.impl.DefaultDiscoveryService}
 * implementation is used. A <tt>DiscoveryService</tt> somehow finds available
 * {@link DiscoveryStrategy}s inside the classpath and manages their activation
 * or deactivation status.
 * <p/>
 * This interface is used by system integrators, integrating Hazelcast into their own
 * frameworks or environments, are free to extend or exchange the default implementation
 * based on their needs and requirements.
 * <p/>
 * Only enabled providers are expected to discover nodes but, depending on the
 * <tt>DiscoveryService</tt> implementation, multiple {@link DiscoveryStrategy}s
 * might be enabled at the same time (e.g. TCP-IP Joiner with well known addresses
 * and Cloud discovery).
 *
 * @since 3.6
 */
public interface DiscoveryService {

    /**
     * The <tt>start</tt> method is called on system startup to implement simple
     * lifecycle management. This method is expected to call
     * {@link DiscoveryStrategy#start()} on all discovered and start up strategies.
     */
    void start();

    /**
     * Returns a discovered and filtered, if a {@link NodeFilter} is setup, set of
     * discovered nodes to connect to.
     *
     * @return a set of discovered and filtered nodes
     */
    Iterable<DiscoveryNode> discoverNodes();

    /**
     * The <tt>start</tt> method is called on system startup to implement simple
     * lifecycle management. This method is expected to call
     * {@link DiscoveryStrategy#destroy()} on all discovered and destroy strategies
     * before the service itself will be destroyed.
     */
    void destroy();

    /**
     * Returns a map with discovered metadata provided by the runtime environment. Those information
     * may include, but are not limited, to location information like datacenter, rack, host or additional
     * tags to be used for custom purpose.
     * <p/>
     * Information discovered from this method are shaded into the {@link com.hazelcast.core.Member}s
     * attributes. Existing attributes will not be overridden, that way local attribute configuration
     * overrides provided metadata.
     * <p/>
     * The default implementation provides an empty map with no further metadata configured. Returning
     * <tt>null</tt> is not permitted and will most probably result in an {@link NullPointerException}
     * inside the cluster system.
     *
     * @return a map of discovered metadata as provided by the runtime environment
     * @since 3.7
     */
    Map<String, Object> discoverLocalMetadata();
}
