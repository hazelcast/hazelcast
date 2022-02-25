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

package com.hazelcast.cluster;

import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.version.MemberVersion;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;

/**
 * Cluster member interface.
 *
 * @see Cluster
 * @see MembershipListener
 */
public interface Member extends DataSerializable, Endpoint {

    /**
     * Returns true if this member is the local member.
     *
     * @return <code>true</code> if this member is the local member, <code>false</code> otherwise.
     */
    boolean localMember();

    /**
     * Returns true if this member is a lite member.
     *
     * @return <code>true</code> if this member is a lite member, <code>false</code> otherwise.
     * Lite members do not own any partition.
     */
    boolean isLiteMember();

    /**
     * Returns the Address of this Member.
     *
     * @return the address.
     * @since 3.6
     */
    Address getAddress();

    /**
     * @return      a map of server socket {@link Address}es per {@link EndpointQualifier} of this member
     * @since 3.12
     */
    Map<EndpointQualifier, Address> getAddressMap();

    /**
     * Returns the socket address of this member for member to member communications or unified depending on config.
     * Equivalent to {@link #getSocketAddress(EndpointQualifier) getSocketAddress(ProtocolType.MEMBER)}.
     *
     * @return the socket address of this member for member to member communications or unified depending on config.
     */
    InetSocketAddress getSocketAddress();

    /**
     * Returns the socket address of this member.
     *
     * @return the socket address of this member
     * @since 3.12
     */
    InetSocketAddress getSocketAddress(EndpointQualifier qualifier);

    /**
     * Returns the UUID of this member.
     *
     * @return the UUID of this member.
     */
    UUID getUuid();

    /**
     * Returns configured attributes for this member.<br>
     * <b>This method might not be available on all native clients.</b>
     *
     * @return configured attributes for this member.
     */
    Map<String, String> getAttributes();

    /**
     * Returns the value of the specified key for this member or
     * null if value is undefined.
     *
     * @param key The key to lookup.
     * @return The value for this member key.
     */
    String getAttribute(String key);

    /**
     * Returns the Hazelcast codebase version of this member; this may or may not be different from the version reported by
     * {@link Cluster#getClusterVersion()}, for example when a node with a different codebase version is added to an
     * existing cluster. See the documentation for {@link Cluster#getClusterVersion()} for a more thorough discussion
     * of {@code Cluster} and {@code Member} / {@code Node} version.
     *
     * @return the {@link MemberVersion} of this member.
     * @since 3.8
     */
    MemberVersion getVersion();

}
