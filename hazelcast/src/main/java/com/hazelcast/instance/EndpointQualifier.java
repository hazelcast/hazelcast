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

import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * An {@code EndpointQualifier} is a {@code (ProtocolType, String)} tuple that uniquely identifies groups
 * of network connections sharing a common {@link ProtocolType} and the same network settings, when Hazelcast
 * is configured with {@link com.hazelcast.config.AdvancedNetworkConfig} enabled.
 * <p>
 * In some cases, just the {@link ProtocolType} is enough (eg since there can be only a single
 * {@link ProtocolType#MEMBER MEMBER} server socket, there can be only one instance of
 * {@link com.hazelcast.config.ServerSocketEndpointConfig ServerSocketEndpointConfig} network
 * configuration for {@code MEMBER} connections).
 * <p>
 * When just the {@link ProtocolType} is not enough (for example when configuring outgoing WAN
 * connections to 2 different target clusters), a {@code String identifier} is used to uniquely
 * identify the network configuration.
 *
 * @see com.hazelcast.config.AdvancedNetworkConfig
 * @see com.hazelcast.config.EndpointConfig
 * @since 3.12
 */
public final class EndpointQualifier
        implements IdentifiedDataSerializable {

    public static final EndpointQualifier MEMBER = new EndpointQualifier(ProtocolType.MEMBER, null);
    public static final EndpointQualifier CLIENT = new EndpointQualifier(ProtocolType.CLIENT, null);
    public static final EndpointQualifier REST = new EndpointQualifier(ProtocolType.REST, null);
    public static final EndpointQualifier MEMCACHE = new EndpointQualifier(ProtocolType.MEMCACHE, null);

    private ProtocolType type;
    private String identifier;

    public EndpointQualifier() {

    }

    private EndpointQualifier(ProtocolType type, String identifier) {
        checkNotNull(type);
        this.type = type;
        this.identifier = identifier;
    }

    public String getIdentifier() {
        return identifier;
    }

    public ProtocolType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EndpointQualifier that = (EndpointQualifier) o;
        if (type != that.type) {
            return false;
        }

        return identifier != null ? identifier.equals(that.identifier) : that.identifier == null;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        if (!isSingleType(type)) {
            result = 31 * result + (identifier != null ? identifier.hashCode() : 0);
        }
        return result;
    }

    public String toMetricsPrefixString() {
        String identifier = this.identifier != null ? this.identifier : "";
        return type.name() + (!isSingleType(type) ? ("-" + identifier.replaceAll("\\s", "_")) : "");
    }

    private static boolean isSingleType(ProtocolType type) {
        return type.getServerSocketCardinality() == 1;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        type = ProtocolType.valueOf(in.readInt());
        identifier = in.readString();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(type.ordinal());
        out.writeString(identifier);
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.ENDPOINT_QUALIFIER;
    }

    @Override
    public String toString() {
        return "EndpointQualifier{"
                + "type='" + type
                + (!isSingleType(type) ? ("', id='" + identifier) : "")
                + '\'' + '}';
    }

    /**
     * @return resolved endpoint qualifier when it is passed from the user via configuration
     */
    public static EndpointQualifier resolveForConfig(ProtocolType protocolType, String identifier) {
        if (ProtocolType.CLIENT.equals(protocolType)) {
            return CLIENT;
        }
        return resolve(protocolType, identifier);
    }

    public static EndpointQualifier resolve(ProtocolType protocolType, String identifier) {
        switch (protocolType) {
            case MEMBER:
                return MEMBER;
            case CLIENT:
                return new EndpointQualifier(ProtocolType.CLIENT, identifier);
            case MEMCACHE:
                return MEMCACHE;
            case REST:
                return REST;
            case WAN:
                return new EndpointQualifier(ProtocolType.WAN, identifier);
            default:
                throw new IllegalArgumentException("Cannot resolve EndpointQualifier for protocol type " + protocolType);
        }
    }
}
