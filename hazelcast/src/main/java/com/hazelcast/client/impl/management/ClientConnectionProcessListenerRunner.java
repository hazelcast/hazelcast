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

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.util.executor.PoolExecutorThreadFactory;
import com.hazelcast.logging.ILogger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Runs all the configured listeners on a specific executor, with proper error
 * handling.
 */
public final class ClientConnectionProcessListenerRunner {

    private final CopyOnWriteArrayList<ClientConnectionProcessListener> listeners;
    private final HazelcastClientInstanceImpl client;
    private final AtomicBoolean hasListeners;
    private volatile ILogger logger;
    private volatile ExecutorService executor;

    public ClientConnectionProcessListenerRunner(HazelcastClientInstanceImpl client) {
        this.client = client;
        listeners = new CopyOnWriteArrayList<>();
        hasListeners = new AtomicBoolean(false);
    }

    /**
     * Adds a new listener to receive the connection diagnostics events.
     * <p>
     * The executor to run the events will only be created lazily, the first
     * time this method is called.
     * <p>
     * Adding listeners should be completed first, before any event is offered
     * to this runner.
     */
    public void addListener(ClientConnectionProcessListener listener) {
        if (hasListeners.compareAndSet(false, true)) {
            createLogger();
            createExecutor();
        }

        listeners.add(listener);
    }

    /**
     * Stops the executor, if it has been created.
     * <p>
     * It waits for the tasks that has been submitted before the call to stop,
     * to not miss firing terminal connection events like cluster connection
     * failed during the client shutdown.
     */
    public void stop() {
        if (!hasListeners.get()) {
            return;
        }

        executor.shutdown();
        ClientExecutionServiceImpl.awaitExecutorTermination("connection-diagnostics", executor, logger);
    }

    /**
     * Calls
     * {@link
     * ClientConnectionProcessListener#attemptingToConnectToAddress(Address)} on
     * all listeners after translating the target address.
     */
    public <A> void onAttemptingToConnectToTarget(Function<A, Address> addressTranslator, A target) {
        if (!hasListeners.get()) {
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
        if (!hasListeners.get()) {
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
        if (!hasListeners.get()) {
            return;
        }

        callListeners(listener -> listener.hostNotFound(host));
    }

    /**
     * Calls
     * {@link ClientConnectionProcessListener#possibleAddressesCollected(List)}
     * on all listeners, after converting the given collection to a list.
     */
    public void onPossibleAddressesCollected(Collection<Address> addresses) {
        if (!hasListeners.get()) {
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
        if (!hasListeners.get()) {
            return;
        }

        callListeners(listener -> listener.possibleAddressesCollected(addresses));
    }

    /**
     * Calls
     * {@link ClientConnectionProcessListener#authenticationSuccess(Address)} on
     * all listeners.
     */
    public void onAuthenticationSuccess(Address address) {
        if (!hasListeners.get()) {
            return;
        }

        callListeners(listener -> listener.authenticationSuccess(address));
    }

    /**
     * Calls {@link ClientConnectionProcessListener#credentialsFailed(Address)}
     * on all listeners.
     */
    public void onCredentialsFailed(Address address) {
        if (!hasListeners.get()) {
            return;
        }

        callListeners(listener -> listener.credentialsFailed(address));
    }

    /**
     * Calls
     * {@link
     * ClientConnectionProcessListener#clientNotAllowedInCluster(Address)} on
     * all listeners.
     */
    public void onClientNotAllowedInCluster(Address address) {
        if (!hasListeners.get()) {
            return;
        }

        callListeners(listener -> listener.clientNotAllowedInCluster(address));
    }

    /**
     * Calls
     * {@link ClientConnectionProcessListener#clusterConnectionFailed(String)}
     * on all listeners.
     */
    public void onClusterConnectionFailed(String clusterName) {
        if (!hasListeners.get()) {
            return;
        }

        callListeners(listener -> listener.clusterConnectionFailed(clusterName));
    }

    /**
     * Calls
     * {@link
     * ClientConnectionProcessListener#clusterConnectionSucceeded(String)} on
     * all listeners.
     */
    public void onClusterConnectionSucceeded(String clusterName) {
        if (!hasListeners.get()) {
            return;
        }

        callListeners(listener -> listener.clusterConnectionSucceeded(clusterName));
    }

    /**
     * Calls
     * {@link ClientConnectionProcessListener#remoteClosedConnection(Address)}
     * on all listeners after translating the target address.
     */
    public <A> void onRemoteClosedConnection(Function<A, Address> addressTranslator, A target) {
        if (!hasListeners.get()) {
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
        try {
            executor.execute(() -> {
                for (ClientConnectionProcessListener listener : listeners) {
                    try {
                        consumer.accept(listener);
                    } catch (Throwable t) {
                        logger.finest("Exception while running the listener " + listener, t);
                    }
                }
            });
        } catch (RejectedExecutionException ignored) {
            // Client is shutting down
        }
    }

    private void createLogger() {
        logger = client.getLoggingService().getLogger(ClientConnectionProcessListenerRunner.class);
    }

    private void createExecutor() {
        ClassLoader classLoader = client.getClientConfig().getClassLoader();
        executor = Executors.newSingleThreadExecutor(
                new PoolExecutorThreadFactory(client.getName() + ".connection-diagnostics-", classLoader));
    }
}
