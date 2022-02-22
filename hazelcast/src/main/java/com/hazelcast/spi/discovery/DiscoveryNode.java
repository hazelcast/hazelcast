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

import com.hazelcast.cluster.Address;

import java.util.Map;

/**
 * A <code>DiscoveryNode</code> describes a nodes addresses (private and if
 * necessary a public one) as well as attributes assigned to this node.
 * Private address defines the typical internal communication port, all
 * cluster communication and also client communication inside the same
 * network happens on this address. However in cloud environments clients
 * might run in other sub-networks and therefore cannot access the private
 * address. In those situations {@link DiscoveryStrategy}
 * vendors can retrieve and assign an additional external (or public) IP
 * address of the node for clients to connect against.
 * <p>
 * Public addresses, if not necessary, can either be returned as null or
 * may return the same address instance as the private one and will be
 * handled internally the same way.
 * <p>
 * The properties will be used to store any kind of tags or other metadata
 * available for the node inside of the cloud environment. If a
 * {@link com.hazelcast.spi.discovery.NodeFilter} is configured, these
 * properties might be used for further refinement of the discovered nodes
 * based on whatever the filter decides.
 * <p>
 * This class is implemented as an abstract class to offer easy extensibility
 * in later versions of the SPI. Since Java only offers forward evolution of
 * interfaces starting with Java 8, this is the best option.
 *
 * @since 3.6
 */
public abstract class DiscoveryNode {

    /**
     * Returns the private address of the discovered node. The private address
     * <b>must not be</b> null.
     *
     * @return the private address of the discovered node
     */
    public abstract Address getPrivateAddress();

    /**
     * Returns the public address of the discovered node if available. Public addresses
     * are optional and this method may return null or the same address as {@link #getPrivateAddress()}.
     *
     * @return the public address of the discovered node if available otherwise null or {@link #getPrivateAddress()}
     */
    public abstract Address getPublicAddress();

    /**
     * Returns a set of unmodifiable properties that are assigned to the discovered node. These properties
     * can be used for additional filtering based on the {@link NodeFilter} API.
     *
     * @return assigned properties of that node
     */
    public abstract Map<String, Object> getProperties();
}
