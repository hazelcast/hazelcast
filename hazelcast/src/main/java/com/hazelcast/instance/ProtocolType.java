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

package com.hazelcast.instance;

import com.hazelcast.internal.nio.Protocols;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Types of server sockets. A member typically responds to several types of protocols
 * for member-to-member, client-member protocol, WAN communication etc. The default
 * configuration uses a single server socket to listen for all kinds of protocol
 * types configured, while {@link com.hazelcast.config.AdvancedNetworkConfig} allows
 * configuration of multiple server sockets.
 *
 * @see com.hazelcast.config.NetworkConfig
 * @see com.hazelcast.config.AdvancedNetworkConfig
 * @since 3.12
 */
public enum ProtocolType {

    /*
     * Ordinals of this enum are used for IDS inside {@link EndpointQualifier}.
     * Do not re-arrange, only append new values at end.
     *
     * Note: names of this enum are used for metrics sent to Management Center, do not rename.
     */
    MEMBER(0, 1, Protocols.CLUSTER),
    CLIENT(1, 1, Protocols.CLIENT_BINARY),
    WAN(2, Integer.MAX_VALUE, Protocols.CLUSTER),
    REST(3, 1, Protocols.REST),
    MEMCACHE(4, 1, Protocols.MEMCACHE);

    private static final Set<ProtocolType> PROTOCOL_TYPES_SET;
    private static final ProtocolType[] PROTOCOL_TYPES;

    static {
        Set<ProtocolType> allProtocolTypes = EnumSet.allOf(ProtocolType.class);
        PROTOCOL_TYPES_SET = Collections.unmodifiableSet(allProtocolTypes);
        PROTOCOL_TYPES = ProtocolType.values();
    }

    private final int id;
    private final int serverSocketCardinality;
    private final String descriptor;

    ProtocolType(int id, int serverSocketCardinality, String descriptor) {
        this.id = id;
        this.serverSocketCardinality = serverSocketCardinality;
        this.descriptor = descriptor;
    }

    public static ProtocolType valueOf(int ordinal) {
        return PROTOCOL_TYPES[ordinal];
    }

    public static Set<ProtocolType> valuesAsSet() {
        return PROTOCOL_TYPES_SET;
    }

    public int getServerSocketCardinality() {
        return serverSocketCardinality;
    }

    public String getDescriptor() {
        return descriptor;
    }

    public int getId() {
        return id;
    }

    public static ProtocolType getById(final int id) {
        for (ProtocolType type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        return null;
    }
}
