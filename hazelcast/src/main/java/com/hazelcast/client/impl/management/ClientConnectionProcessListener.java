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

package com.hazelcast.client.impl.management;

import com.hazelcast.cluster.Address;

import java.util.EventListener;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Arrays.asList;

final class ListenerAggregate
        implements ClientConnectionProcessListener {

    private final List<ClientConnectionProcessListener> childListeners;

    public ListenerAggregate(ClientConnectionProcessListener... listeners) {
        childListeners = new CopyOnWriteArrayList<>(asList(listeners));
    }

    @Override
    public void attemptingToConnectToAddress(Address address) {
        childListeners.forEach(listener -> listener.attemptingToConnectToAddress(address));
    }

    @Override
    public void connectionAttemptFailed(Address target) {
        childListeners.forEach(listener -> listener.connectionAttemptFailed(target));
    }

    @Override
    public void hostNotFound(String host) {
        childListeners.forEach(listener -> listener.hostNotFound(host));
    }

    @Override
    public void possibleAddressesCollected(List<Address> addresses) {
        childListeners.forEach(listener -> listener.possibleAddressesCollected(addresses));
    }

    @Override
    public void authenticationSuccess() {
        childListeners.forEach(ClientConnectionProcessListener::authenticationSuccess);
    }

    @Override
    public void credentialsFailed() {
        childListeners.forEach(ClientConnectionProcessListener::credentialsFailed);
    }

    @Override
    public void clientNotAllowedInCluster() {
        childListeners.forEach(ClientConnectionProcessListener::clientNotAllowedInCluster);
    }

    @Override
    public void clusterConnectionFailed(String clusterName) {
        childListeners.forEach(listener -> listener.clusterConnectionFailed(clusterName));
    }

    @Override
    public void clusterConnectionSucceeded(String clusterName) {
        childListeners.forEach(listener -> listener.clusterConnectionSucceeded(clusterName));
    }

    @Override
    public void remoteClosedConnection(Address address) {
        childListeners.forEach(listener -> listener.remoteClosedConnection(address));
    }

    @Override
    public ClientConnectionProcessListener withAdditionalListener(ClientConnectionProcessListener listener) {
        childListeners.add(listener);
        return this;
    }
}

public interface ClientConnectionProcessListener
        extends EventListener {

    ClientConnectionProcessListener NOOP = new ClientConnectionProcessListener() {

        @Override
        public ClientConnectionProcessListener withAdditionalListener(ClientConnectionProcessListener listener) {
            return listener;
        }
    };

    default void attemptingToConnectToAddress(Address address) {
    }

    default void connectionAttemptFailed(Address target) {
    }

    default void hostNotFound(String host) {
    }

    default void possibleAddressesCollected(List<Address> addresses) {
    }

    default ClientConnectionProcessListener withAdditionalListener(ClientConnectionProcessListener listener) {
        return new ListenerAggregate(this, listener);
    }

    default void authenticationSuccess() {
    }

    default void credentialsFailed() {
    }

    default void clientNotAllowedInCluster() {
    }

    default void clusterConnectionFailed(String clusterName) {
    }

    default void clusterConnectionSucceeded(String clusterName) {
    }

    default void remoteClosedConnection(Address address) {
    }
}
