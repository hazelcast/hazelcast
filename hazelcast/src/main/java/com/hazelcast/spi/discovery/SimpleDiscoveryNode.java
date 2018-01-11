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

package com.hazelcast.spi.discovery;

import com.hazelcast.nio.Address;

import java.util.Collections;
import java.util.Map;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.unmodifiableMap;

/**
 * Simple immutable implementation of the {@link DiscoveryNode} interface for convenience
 * when implementing a {@link DiscoveryStrategy}.
 *
 * @since 3.6
 */
public final class SimpleDiscoveryNode
        extends DiscoveryNode {

    private final Address privateAddress;
    private final Address publicAddress;
    private final Map<String, Object> properties;

    /**
     * This constructor will set private and public addresses to the same value and no properties
     * are available.
     *
     * @param privateAddress the discovered node's private address
     */
    public SimpleDiscoveryNode(Address privateAddress) {
        this(privateAddress, privateAddress, Collections.<String, Object>emptyMap());
    }

    /**
     * This constructor will set private and public addresses to the same value.
     *
     * @param privateAddress the discovered node's private address
     * @param properties     the discovered node's additional properties
     */
    public SimpleDiscoveryNode(Address privateAddress, Map<String, Object> properties) {
        this(privateAddress, privateAddress, properties);
    }

    /**
     * <p>This constructor will set private and public addresses separately and no properties are available.
     * Based on the internal implementation Hazelcast will either choose private or public address to connect
     * to the cluster.</p>
     * <p>On members private addresses are preferred.</p>
     *
     * @param privateAddress the discovered node's private address
     * @param publicAddress  the discovered node's public address
     */
    public SimpleDiscoveryNode(Address privateAddress, Address publicAddress) {
        this(privateAddress, publicAddress, Collections.<String, Object>emptyMap());
    }

    /**
     * <p>This constructor will set private and public addresses separately. Based on the internal
     * implementation Hazelcast will either choose private or public address to connect to the cluster.</p>
     * <p>On members private addresses are preferred.</p>
     *
     * @param privateAddress the discovered node's private address
     * @param publicAddress  the discovered node's public address
     * @param properties     the discovered node's additional properties
     */
    public SimpleDiscoveryNode(Address privateAddress, Address publicAddress, Map<String, Object> properties) {
        checkNotNull(privateAddress, "The private address cannot be null");
        checkNotNull(properties, "The properties cannot be null");
        this.privateAddress = privateAddress;
        this.publicAddress = publicAddress;
        this.properties = unmodifiableMap(properties);
    }

    @Override
    public Address getPrivateAddress() {
        return privateAddress;
    }

    @Override
    public Address getPublicAddress() {
        return publicAddress;
    }

    @Override
    public Map<String, Object> getProperties() {
        return properties;
    }
}
