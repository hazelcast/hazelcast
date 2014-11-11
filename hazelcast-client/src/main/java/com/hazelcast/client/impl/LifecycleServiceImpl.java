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

package com.hazelcast.client.impl;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.UuidUtil;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTDOWN;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTTING_DOWN;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTING;

/**
 * Default {@link com.hazelcast.core.LifecycleService} implementation for the client.
 */
public final class LifecycleServiceImpl implements LifecycleService {

    private final HazelcastClientInstanceImpl client;
    private final ConcurrentMap<String, LifecycleListener> lifecycleListeners
            = new ConcurrentHashMap<String, LifecycleListener>();
    private final AtomicBoolean active = new AtomicBoolean(false);
    private final BuildInfo buildInfo;

    public LifecycleServiceImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
        final List<ListenerConfig> listenerConfigs = client.getClientConfig().getListenerConfigs();
        if (listenerConfigs != null && !listenerConfigs.isEmpty()) {
            for (ListenerConfig listenerConfig : listenerConfigs) {
                if (listenerConfig.getImplementation() instanceof LifecycleListener) {
                    addLifecycleListener((LifecycleListener) listenerConfig.getImplementation());
                }
            }
        }
        buildInfo = BuildInfoProvider.getBuildInfo();
        fireLifecycleEvent(STARTING);
    }

    private ILogger getLogger() {
        return Logger.getLogger(LifecycleService.class);
    }

    @Override
    public String addLifecycleListener(LifecycleListener lifecycleListener) {
        final String id = UuidUtil.buildRandomUuidString();
        lifecycleListeners.put(id, lifecycleListener);
        return id;
    }

    @Override
    public boolean removeLifecycleListener(String registrationId) {
        return lifecycleListeners.remove(registrationId) != null;
    }

    public void fireLifecycleEvent(LifecycleEvent.LifecycleState lifecycleState) {
        final LifecycleEvent lifecycleEvent = new LifecycleEvent(lifecycleState);
        getLogger().info("HazelcastClient[" + client.getName() + "]" + "["
                + buildInfo.getVersion() + "] is " + lifecycleEvent.getState());
        for (LifecycleListener lifecycleListener : lifecycleListeners.values()) {
            lifecycleListener.stateChanged(lifecycleEvent);
        }
    }

    public void setStarted() {
        active.set(true);
        fireLifecycleEvent(STARTED);
    }

    @Override
    public boolean isRunning() {
        return active.get();
    }

    @Override
    public void shutdown() {
        if (!active.compareAndSet(true, false)) {
            return;
        }

        fireLifecycleEvent(SHUTTING_DOWN);
        HazelcastClient.shutdown(client.getName());
        client.doShutdown();
        fireLifecycleEvent(SHUTDOWN);
    }

    @Override
    public void terminate() {
        shutdown();
    }
}
