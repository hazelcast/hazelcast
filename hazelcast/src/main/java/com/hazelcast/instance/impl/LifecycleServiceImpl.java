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

package com.hazelcast.instance.impl;

import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.internal.jmx.ManagementService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.UuidUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTDOWN;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTTING_DOWN;

public class LifecycleServiceImpl implements LifecycleService {

    private final HazelcastInstanceImpl instance;
    private final ConcurrentMap<String, LifecycleListener> lifecycleListeners
            = new ConcurrentHashMap<String, LifecycleListener>();
    private final Object lifecycleLock = new Object();

    public LifecycleServiceImpl(HazelcastInstanceImpl instance) {
        this.instance = instance;
    }

    private ILogger getLogger() {
        return instance.node.getLogger(LifecycleService.class.getName());
    }

    @Override
    public String addLifecycleListener(LifecycleListener lifecycleListener) {
        final String id = UuidUtil.newUnsecureUuidString();
        lifecycleListeners.put(id, lifecycleListener);
        return id;
    }

    @Override
    public boolean removeLifecycleListener(String registrationId) {
        return lifecycleListeners.remove(registrationId) != null;
    }

    public void fireLifecycleEvent(LifecycleState lifecycleState) {
        fireLifecycleEvent(new LifecycleEvent(lifecycleState));
    }

    public void fireLifecycleEvent(LifecycleEvent lifecycleEvent) {
        getLogger().info(instance.node.getThisAddress() + " is " + lifecycleEvent.getState());
        for (LifecycleListener lifecycleListener : lifecycleListeners.values()) {
            lifecycleListener.stateChanged(lifecycleEvent);
        }
    }

    @Override
    public boolean isRunning() {
        //no synchronization needed since getState() is atomic.
        return instance.node.isRunning();
    }

    @Override
    public void shutdown() {
        shutdown(false);
    }

    @Override
    public void terminate() {
        shutdown(true);
    }

    private void shutdown(boolean terminate) {
        synchronized (lifecycleLock) {
            fireLifecycleEvent(SHUTTING_DOWN);
            ManagementService managementService = instance.managementService;
            if (managementService != null) {
                managementService.destroy();
            }
            final Node node = instance.node;
            if (node != null) {
                node.shutdown(terminate);
            }
            HazelcastInstanceFactory.remove(instance);
            fireLifecycleEvent(SHUTDOWN);
        }
    }

    public void runUnderLifecycleLock(final Runnable runnable) {
        synchronized (lifecycleLock) {
            runnable.run();
        }
    }
}
