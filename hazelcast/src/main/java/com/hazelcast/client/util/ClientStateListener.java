/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.util;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_CONNECTED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTDOWN;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTTING_DOWN;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTING;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Helper class for the user to track the lifecycle state of the client.
 * The user will instantiate this listener and it will be registered to the client configuration.
 * If the provided client config is not used while instantiating the client, this helper class
 * will not be useful. It is the user's responsibility to instantiate the client with the same
 * ClientConfig which was used to instantiate this helper.
 */
public class ClientStateListener
        implements LifecycleListener {
    private LifecycleEvent.LifecycleState currentState = STARTING;

    private final Lock lock = new ReentrantLock();
    private final Condition connectedCondition = lock.newCondition();
    private final Condition disconnectedCondition = lock.newCondition();

    /**
     * Registers this instance with the provided client configuration
     *
     * @param clientConfig The client configuration to which this listener will be registered
     */
    public ClientStateListener(ClientConfig clientConfig) {
        clientConfig.addListenerConfig(new ListenerConfig(this));
    }

    @Override
    public void stateChanged(LifecycleEvent event) {
        lock.lock();
        try {
            currentState = event.getState();
            if (currentState.equals(CLIENT_CONNECTED) || currentState.equals(SHUTTING_DOWN) || currentState.equals(SHUTDOWN)) {
                connectedCondition.signalAll();
            }
            if (currentState.equals(CLIENT_DISCONNECTED) || currentState.equals(SHUTTING_DOWN) || currentState
                    .equals(SHUTDOWN)) {
                disconnectedCondition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits until the client is connected to cluster or the timeout expires.
     * Does not wait if the client is already shutting down or shutdown.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the {@code timeout} argument
     * @return true if the client is connected to the cluster. On returning false,
     * you can check if timeout occured or the client is shutdown using {@code isShutdown} {@code getCurrentState}
     * @throws InterruptedException
     */
    public boolean awaitConnected(long timeout, TimeUnit unit)
            throws InterruptedException {
        lock.lock();
        try {
            if (currentState.equals(CLIENT_CONNECTED)) {
                return true;
            }

            if (currentState.equals(SHUTTING_DOWN) || currentState.equals(SHUTDOWN)) {
                return false;
            }

            long duration = unit.toNanos(timeout);
            while (duration > 0) {
                duration = connectedCondition.awaitNanos(duration);

                if (currentState.equals(CLIENT_CONNECTED)) {
                    return true;
                }
            }

            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits until the client is connected to cluster.
     * Does not wait if the client is already shutting down or shutdown.
     *
     * @return returns whatever {@code awaitConnected(long timeout, TimeUnit unit)} returns.
     * @throws InterruptedException
     */
    public boolean awaitConnected()
            throws InterruptedException {
        return awaitConnected(Long.MAX_VALUE, MILLISECONDS);
    }

    /**
     * Waits until the client is disconnected from the cluster or the timeout expires.
     * Does not wait if the client is already shutting down or shutdown.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the {@code timeout} argument
     * @return true if the client is disconnected to the cluster. On returning false,
     * you can check if timeout occured or the client is shutdown using {@code isShutdown} {@code getCurrentState}
     * @throws InterruptedException
     */
    public boolean awaitDisconnected(long timeout, TimeUnit unit)
            throws InterruptedException {
        lock.lock();
        try {
            if (currentState.equals(CLIENT_DISCONNECTED) || currentState.equals(SHUTTING_DOWN) || currentState.equals(SHUTDOWN)) {
                return true;
            }

            long duration = unit.toNanos(timeout);
            while (duration > 0) {
                duration = disconnectedCondition.awaitNanos(duration);

                if (currentState.equals(CLIENT_DISCONNECTED) || currentState.equals(SHUTTING_DOWN) || currentState
                        .equals(SHUTDOWN)) {
                    return true;
                }
            }

            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits until the client is disconnected from the cluster.
     * Does not wait if the client is already shutting down or shutdown.
     *
     * @return returns whatever {@code awaitDisconnected(long timeout, TimeUnit unit)} returns.
     * @throws InterruptedException
     */
    public boolean awaitDisconnected()
            throws InterruptedException {
        return awaitDisconnected(Long.MAX_VALUE, MILLISECONDS);
    }

    /**
     * @return true if the client is connected.
     */
    public boolean isConnected() {
        lock.lock();
        try {
            return currentState.equals(CLIENT_CONNECTED);
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return true if the client is shutdown.
     */
    public boolean isShutdown() {
        lock.lock();
        try {
            return currentState.equals(SHUTDOWN);
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return true if the client is started.
     */
    public boolean isStarted() {
        lock.lock();
        try {
            return currentState.equals(STARTED) || currentState.equals(CLIENT_CONNECTED) || currentState
                    .equals(CLIENT_DISCONNECTED);
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return The current lifecycle state of the client.
     */
    public LifecycleEvent.LifecycleState getCurrentState() {
        lock.lock();
        try {
            return currentState;
        } finally {
            lock.unlock();
        }
    }
}
