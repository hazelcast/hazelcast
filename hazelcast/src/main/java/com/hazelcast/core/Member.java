/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.nio.DataSerializable;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Cluster member interface. The default implementation
 * {@link com.hazelcast.impl.MemberImpl} violates the Java Serialization contract.
 * It should be serialized/deserialized by Hazelcast.
 *
 * @see Cluster
 * @see MembershipListener
 */
public interface Member extends DataSerializable {

    /**
     * Returns if this member is the local member.
     *
     * @return <tt>true<tt> if this member is the
     *         local member, <tt>false</tt> otherwise.
     */
    boolean localMember();

    /**
     * Returns the port number of this member.
     * <p/>
     * Each member in the cluster has a server socket.
     *
     * @return port number of this member.
     * @deprecated use @link{#getInetSocketAddress()}
     */
    @Deprecated
    int getPort();

    /**
     * Returns the InetAddress of this member.
     *
     * @return InetAddress of this member
     * @deprecated use @link{#getInetSocketAddress()}
     */
    @Deprecated
    InetAddress getInetAddress();

    /**
     * Returns the InetSocketAddress of this member.
     *
     * @return InetSocketAddress of this member
     */
    InetSocketAddress getInetSocketAddress();

    /**
     * Returns if this member is a LiteMember.
     * LiteMember is a cluster member which doesn't
     * hold any data on it. We never use the term
     * 'Super Client' as of version 2.0 as it is misleading.
     *
     * @return <tt>true</tt> if this member is a super
     *         client, <tt>false</tt> otherwise
     * @see #isLiteMember()
     * @deprecated as of version 2.0
     */
    @Deprecated
    boolean isSuperClient();

    /**
     * Returns if this member is a LiteMember.
     * LiteMember is a cluster member which doesn't
     * hold any data on it.
     *
     * @return <tt>true</tt> if this member is a LiteMember,
     *         tt>false</tt> otherwise
     */
    boolean isLiteMember();


    /**
     * Returns UUID of this member.
     *
     * @return UUID of this member.
     */
    public String getUuid();
}
