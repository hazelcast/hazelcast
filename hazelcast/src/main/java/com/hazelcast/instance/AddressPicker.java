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

package com.hazelcast.instance;

import com.hazelcast.nio.Address;

import java.nio.channels.ServerSocketChannel;

/**
 * Strategy to select an {@link Address} that a Hazelcast cluster member binds its server socket to
 * and a (potentially different) address that Hazelcast will advertise to other cluster members and clients.
 *
 */
public interface AddressPicker {

    /**
     * Picks both server socket listener address and public address.
     *
     * @throws Exception
     */
    void pickAddress() throws Exception;

    /**
     * Returns a server socket listener address.
     *
     * @return {@link Address} where the server socket was bound to or <code>null</code> if called before.
     * {@link #pickAddress()}
     */
    Address getBindAddress();

    /**
     * Returns a public address to be advertised to other cluster members and clients.
     *
     * @return {@link Address} another members can use to connect to this member or <code>null</code> if called before
     * {@link #pickAddress()}
     */
    Address getPublicAddress();

    /**
     * Returns a server channel.
     *
     * @return <code>ServerSocketChannel</code> to be listened to by an acceptor or <code>null</code> if called before
     * {@link #pickAddress()}
     */
    ServerSocketChannel getServerSocketChannel();
}
