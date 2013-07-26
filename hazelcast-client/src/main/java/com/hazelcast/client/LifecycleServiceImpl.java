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

package com.hazelcast.client;

import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Collection;
import java.util.EventListener;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.*;

/**
 * @author mdogan 5/16/13
 */
public final class LifecycleServiceImpl implements LifecycleService {

    private final HazelcastClient client;
    private final ConcurrentMap<String, LifecycleListener> lifecycleListeners = new ConcurrentHashMap<String, LifecycleListener>();
    private final Object lifecycleLock = new Object();
    private final AtomicBoolean active = new AtomicBoolean(false);

    public LifecycleServiceImpl(HazelcastClient client) {
        this.client = client;
        final Collection<EventListener> listeners = client.getClientConfig().getListeners();
        if (listeners != null && !listeners.isEmpty()) {
            for (EventListener listener : listeners) {
                if (listener instanceof LifecycleListener) {
                    addLifecycleListener((LifecycleListener) listener);
                }
            }
        }
        fireLifecycleEvent(STARTING);
    }

    private ILogger getLogger() {
        return Logger.getLogger(LifecycleService.class);
    }

    public String addLifecycleListener(LifecycleListener lifecycleListener) {
        final String id = UUID.randomUUID().toString();
        lifecycleListeners.put(id, lifecycleListener);
        return id;
    }

    public boolean removeLifecycleListener(String registrationId) {
        return lifecycleListeners.remove(registrationId) != null;
    }

    private void fireLifecycleEvent(LifecycleEvent.LifecycleState lifecycleState) {
        final LifecycleEvent lifecycleEvent = new LifecycleEvent(lifecycleState);
        getLogger().info("HazelcastClient[" + client.getName() + "] is " + lifecycleEvent.getState());
        for (LifecycleListener lifecycleListener : lifecycleListeners.values()) {
            lifecycleListener.stateChanged(lifecycleEvent);
        }
    }

    void setStarted() {
        active.set(true);
        fireLifecycleEvent(STARTED);
    }

    public boolean isRunning() {
        return active.get();
    }

    public void shutdown() {
        active.set(false);
        synchronized (lifecycleLock) {
            fireLifecycleEvent(SHUTTING_DOWN);
            client.shutdown();
            fireLifecycleEvent(SHUTDOWN);
        }
    }

    public void terminate() {
        shutdown();
    }
}
