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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.*;

public class LifecycleServiceImpl implements LifecycleService {
    final HazelcastInstanceImpl instance;
    final AtomicBoolean paused = new AtomicBoolean(false);
    final List<LifecycleListener> lsLifecycleListeners = new CopyOnWriteArrayList<LifecycleListener>();
    final Object lifecycleLock = new Object();

    public LifecycleServiceImpl(HazelcastInstanceImpl instance) {
        this.instance = instance;
    }

    private ILogger getLogger() {
        return instance.node.getLogger(LifecycleServiceImpl.class.getName());
    }

    public void addLifecycleListener(LifecycleListener lifecycleListener) {
        lsLifecycleListeners.add(lifecycleListener);
    }

    public void removeLifecycleListener(LifecycleListener lifecycleListener) {
        lsLifecycleListeners.remove(lifecycleListener);
    }

    public void fireLifecycleEvent(LifecycleState lifecycleState) {
        fireLifecycleEvent(new LifecycleEvent(lifecycleState));
    }

    public void fireLifecycleEvent(LifecycleEvent lifecycleEvent) {
        getLogger().log(Level.INFO, instance.node.getThisAddress() + " is " + lifecycleEvent.getState());
        for (LifecycleListener lifecycleListener : lsLifecycleListeners) {
            lifecycleListener.stateChanged(lifecycleEvent);
        }
    }

    public boolean pause() {
        synchronized (lifecycleLock) {
            if (!paused.get()) {
                fireLifecycleEvent(PAUSING);
            } else {
                return false;
            }
            paused.set(true);
            fireLifecycleEvent(PAUSED);
            return true;
        }
    }

    public boolean resume() {
        synchronized (lifecycleLock) {
            if (paused.get()) {
                fireLifecycleEvent(RESUMING);
            } else {
                return false;
            }
            paused.set(false);
            fireLifecycleEvent(RESUMED);
            return true;
        }
    }

    public boolean isPaused() {
        return paused.get();
    }

    public boolean isRunning() {
        synchronized (lifecycleLock) {
            return instance.node.isActive();
        }
    }

    public void shutdown() {
        synchronized (lifecycleLock) {
            fireLifecycleEvent(SHUTTING_DOWN);
            instance.managementService.destroy();
            instance.node.shutdown(false, true);
            HazelcastInstanceFactory.remove(instance);
            fireLifecycleEvent(SHUTDOWN);
        }
    }

    public void kill() {
        synchronized (lifecycleLock) {
            fireLifecycleEvent(SHUTTING_DOWN);
            instance.managementService.destroy();
            instance.node.shutdown(true, true);
            HazelcastInstanceFactory.remove(instance);
            fireLifecycleEvent(SHUTDOWN);
        }
    }

    public void restart() {
        synchronized (lifecycleLock) {
            fireLifecycleEvent(RESTARTING);
            paused.set(true);
            final Node node = instance.node;

            node.onRestart();
            node.connectionManager.onRestart();
            node.clusterService.onRestart();
            node.partitionService.onRestart();
            node.rejoin();
            paused.set(false);
            fireLifecycleEvent(RESTARTED);
        }
    }
}
