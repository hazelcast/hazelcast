/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.management;

import com.hazelcast.cluster.Address;

import java.util.EventListener;
import java.util.List;

/**
 * Implementations can be attached to a
 * {@link com.hazelcast.client.impl.connection.tcp.TcpClientConnectionManager}
 * to receive fine-grained events about the client-to-cluster connection
 * process.
 *
 * @since 5.2
 */
public interface ClientConnectionProcessListener extends EventListener {

    /**
     * Triggered before trying to connect to an address. This event is triggered
     * after a {@link #possibleAddressesCollected(List)} event, and the
     * {@code address} parameter is always an element of the possible-addresses
     * list (passed to {@link #possibleAddressesCollected(List)}).
     * <p>
     * If the connection fails, this event is followed by either a
     * {@link #remoteClosedConnection(Address)} or a
     * {@link #connectionAttemptFailed(Address)} event.
     * <p>
     * If the connection is established but the client runs into an
     * authentication failure, then this will be indicated by a subsequent
     * {@link #clientNotAllowedInCluster(Address)} or a
     * {@link #credentialsFailed(Address)} event.
     * <p>
     * If the authentication succeeds then a
     * {@link #authenticationSuccess(Address)} event will be fired, followed by
     * an {@link #clusterConnectionSucceeded(String)}.
     */
    default void attemptingToConnectToAddress(Address address) {
    }

    /**
     * Triggered when an {@code IOException} is thrown during establishing
     * network connection to a member address. This can happen due to a closed
     * port or when no server is listening on the port. Address resolution
     * failure cannot cause this event to be triggered.
     */
    default void connectionAttemptFailed(Address target) {
    }

    /**
     * Triggered when an {@link java.net.UnknownHostException} is thrown during
     * establishing connection to the cluster. It happens while the client
     * collects the possible member addresses, so a {@code hostNotFound()} event
     * is triggered before the {@link #possibleAddressesCollected(List)} event.
     * Can be called multiple times with the same {@code host} parameter.
     */
    default void hostNotFound(String host) {
    }

    /**
     * Triggered once the available addresses are collected by a discovery
     * plugin or fixed address list. Once this event is fired, the client will
     * attempt to connect to the members one by one, hence a sequence of
     * {@link #attemptingToConnectToAddress(Address)} calls will happen.
     */
    default void possibleAddressesCollected(List<Address> addresses) {
    }

    /**
     * Triggered when the client received acknowledgement of successful
     * authentication from a member.
     */
    default void authenticationSuccess(Address remoteAddress) {
    }

    /**
     * Triggered after an {@link #attemptingToConnectToAddress(Address)} event
     * if the member doesn't accept the credentials presented by the client.
     */
    default void credentialsFailed(Address remoteAddress) {
    }

    /**
     * Called after an {@link #attemptingToConnectToAddress(Address)} event if
     * the client gets rejected due to a client filtering rule (see
     * <a
     * href="https://docs.hazelcast.com/management-center/latest/clusters/client-filtering">Client
     * Filtering</a>).
     */
    default void clientNotAllowedInCluster(Address remoteAddress) {
    }

    /**
     * Called when connection to a candidate cluster failed & could not
     * establish connection with any members. The failure reasons are indicated
     * by previously triggered failure events.
     * <p>
     * This failure can be followed by connection attempt to a backup cluster.
     */
    default void clusterConnectionFailed(String clusterName) {
    }

    /**
     * Triggered after connection to at least one cluster member is
     * established.
     */
    default void clusterConnectionSucceeded(String clusterName) {
    }

    /**
     * Called when during establishing the initial connection, the remote side
     * unexpectedly closes the network connection.
     * <p>
     * This can be triggered after an
     * {@link #attemptingToConnectToAddress(Address)} event (with the same
     * address).
     */
    default void remoteClosedConnection(Address address) {
    }
}
