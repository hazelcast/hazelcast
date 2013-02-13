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

import com.hazelcast.nio.serialization.DataSerializable;

import java.net.InetSocketAddress;

/**
 * Cluster member interface. The default implementation
 * {@link com.hazelcast.instance.MemberImpl} violates the Java Serialization contract.
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
     * Returns the InetSocketAddress of this member.
     *
     * @return InetSocketAddress of this member
     */
    InetSocketAddress getInetSocketAddress();

    /**
     * Returns UUID of this member.
     *
     * @return UUID of this member.
     */
    public String getUuid();
}
