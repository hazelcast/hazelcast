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

package com.hazelcast.instance;

import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.UuidUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTDOWN;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTTING_DOWN;

public class LifecycleServiceImpl implements LifecycleService {
    private final HazelcastInstanceImpl instance;
    private final ConcurrentMap<String, LifecycleListener> lifecycleListeners = new ConcurrentHashMap<String, LifecycleListener>();
    private final Object lifecycleLock = new Object();

    public LifecycleServiceImpl(HazelcastInstanceImpl instance) {
        this.instance = instance;
    }

    private ILogger getLogger() {
        return instance.node.getLogger(LifecycleService.class.getName());
    }

    public String addLifecycleListener(LifecycleListener lifecycleListener) {
        final String id = UuidUtil.buildRandomUuidString();
        lifecycleListeners.put(id, lifecycleListener);
        return id;
    }

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

    public boolean isRunning() {
        //no synchronization needed since isActive is threadsafe.
        return instance.node.isActive();
    }

    public void shutdown() {
        synchronized (lifecycleLock) {
            fireLifecycleEvent(SHUTTING_DOWN);
            instance.managementService.destroy();
            final Node node = instance.node;
            if (node != null) {
                final NodeShutdownLatch shutdownLatch = new NodeShutdownLatch(node);
                node.shutdown(false);
                HazelcastInstanceFactory.remove(instance);
                shutdownLatch.await(Math.min(30, node.groupProperties.GRACEFUL_SHUTDOWN_MAX_WAIT.getInteger()), TimeUnit.SECONDS);
            }
            fireLifecycleEvent(SHUTDOWN);
        }
    }

    public void terminate() {
        synchronized (lifecycleLock) {
            fireLifecycleEvent(SHUTTING_DOWN);
            instance.managementService.destroy();
            final Node node = instance.node;
            if (node != null) {
                node.shutdown(true);
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
