/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Stores all the configured listeners and calls them, with proper error handling.
 */
public final class ClientConnectionProcessListenerRegistry {

    private final CopyOnWriteArrayList<ClientConnectionProcessListener> listeners;
    private final ILogger logger;

    public ClientConnectionProcessListenerRegistry(HazelcastClientInstanceImpl client) {
        logger = client.getLoggingService().getLogger(ClientConnectionProcessListenerRegistry.class);
        listeners = new CopyOnWriteArrayList<>();
    }

    /**
     * Adds a new listener to receive the connection diagnostics events.
     */
    public void addListener(ClientConnectionProcessListener listener) {
        listeners.add(listener);
    }

    /**
     * Calls
     * {@link
     * ClientConnectionProcessListener#attemptingToConnectToAddress(Address)} on
     * all listeners after translating the target address.
     */
    public <A> void onAttemptingToConnectToTarget(Function<A, Address> addressTranslator, A target) {
        if (listeners.isEmpty()) {
            return;
        }

        Address translatedAddress = translateAddress(addressTranslator, target, "attemptingToConnectToAddress");
        if (translatedAddress == null) {
            return;
        }

        callListeners(listener -> listener.attemptingToConnectToAddress(translatedAddress));
    }

    /**
     * Calls
     * {@link ClientConnectionProcessListener#connectionAttemptFailed(Address)}
     * on all listeners after translating the target address.
     */
    public <A> void onConnectionAttemptFailed(Function<A, Address> addressTranslator, A target) {
        if (listeners.isEmpty()) {
            return;
        }

        Address translatedAddress = translateAddress(addressTranslator, target, "connectionAttemptFailed");
        if (translatedAddress == null) {
            return;
        }

        callListeners(listener -> listener.connectionAttemptFailed(translatedAddress));
    }

    /**
     * Calls {@link ClientConnectionProcessListener#hostNotFound(String)} on all
     * listeners.
     */
    public void onHostNotFound(String host) {
        callListeners(listener -> listener.hostNotFound(host));
    }

    /**
     * Calls
     * {@link ClientConnectionProcessListener#possibleAddressesCollected(List)}
     * on all listeners, after converting the given collection to a list.
     */
    public void onPossibleAddressesCollected(Collection<Address> addresses) {
        if (listeners.isEmpty()) {
            return;
        }

        ArrayList<Address> addressesList = new ArrayList<>(addresses);
        callListeners(listener -> listener.possibleAddressesCollected(addressesList));
    }

    /**
     * Calls
     * {@link ClientConnectionProcessListener#possibleAddressesCollected(List)}
     * on all listeners.
     */
    public void onPossibleAddressesCollected(List<Address> addresses) {
        callListeners(listener -> listener.possibleAddressesCollected(addresses));
    }

    /**
     * Calls
     * {@link ClientConnectionProcessListener#authenticationSuccess(Address)} on
     * all listeners.
     */
    public void onAuthenticationSuccess(Address address) {
        callListeners(listener -> listener.authenticationSuccess(address));
    }

    /**
     * Calls {@link ClientConnectionProcessListener#credentialsFailed(Address)}
     * on all listeners.
     */
    public void onCredentialsFailed(Address address) {
        callListeners(listener -> listener.credentialsFailed(address));
    }

    /**
     * Calls
     * {@link
     * ClientConnectionProcessListener#clientNotAllowedInCluster(Address)} on
     * all listeners.
     */
    public void onClientNotAllowedInCluster(Address address) {
        callListeners(listener -> listener.clientNotAllowedInCluster(address));
    }

    /**
     * Calls
     * {@link ClientConnectionProcessListener#clusterConnectionFailed(String)}
     * on all listeners.
     */
    public void onClusterConnectionFailed(String clusterName) {
        callListeners(listener -> listener.clusterConnectionFailed(clusterName));
    }

    /**
     * Calls
     * {@link
     * ClientConnectionProcessListener#clusterConnectionSucceeded(String)} on
     * all listeners.
     */
    public void onClusterConnectionSucceeded(String clusterName) {
        callListeners(listener -> listener.clusterConnectionSucceeded(clusterName));
    }

    /**
     * Calls
     * {@link ClientConnectionProcessListener#remoteClosedConnection(Address)}
     * on all listeners after translating the target address.
     */
    public <A> void onRemoteClosedConnection(Function<A, Address> addressTranslator, A target) {
        if (listeners.isEmpty()) {
            return;
        }

        Address translatedAddress = translateAddress(addressTranslator, target, "remoteClosedConnection");
        if (translatedAddress == null) {
            return;
        }

        callListeners(listener -> listener.remoteClosedConnection(translatedAddress));
    }

    private <A> Address translateAddress(Function<A, Address> addressTranslator, A target, String listenerMethodName) {
        try {
            return addressTranslator.apply(target);
        } catch (Throwable t) {
            logger.finest("Failed to translate address, can't fire "
                    + listenerMethodName + " event for target " + target, t);
            return null;
        }
    }

    private void callListeners(Consumer<ClientConnectionProcessListener> consumer) {
        for (ClientConnectionProcessListener listener : listeners) {
            try {
                consumer.accept(listener);
            } catch (Throwable t) {
                logger.finest("Exception while calling the ClientConnectionProcessListener listener " + listener, t);
            }
        }
    }
}
