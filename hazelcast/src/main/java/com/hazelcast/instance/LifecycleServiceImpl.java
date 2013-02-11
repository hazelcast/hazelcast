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
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.logging.Level;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.*;

public class LifecycleServiceImpl implements LifecycleService {
    private final HazelcastInstanceImpl instance;
    private final List<LifecycleListener> lifecycleListeners = new CopyOnWriteArrayList<LifecycleListener>();
    private final Object lifecycleLock = new Object();

    public LifecycleServiceImpl(HazelcastInstanceImpl instance) {
        this.instance = instance;
    }

    private ILogger getLogger() {
        return instance.node.getLogger(LifecycleServiceImpl.class.getName());
    }

    public void addLifecycleListener(LifecycleListener lifecycleListener) {
        lifecycleListeners.add(lifecycleListener);
    }

    public void removeLifecycleListener(LifecycleListener lifecycleListener) {
        lifecycleListeners.remove(lifecycleListener);
    }

    public void fireLifecycleEvent(LifecycleState lifecycleState) {
        fireLifecycleEvent(new LifecycleEvent(lifecycleState));
    }

    public void fireLifecycleEvent(LifecycleEvent lifecycleEvent) {
        getLogger().log(Level.INFO, instance.node.getThisAddress() + " is " + lifecycleEvent.getState());
        for (LifecycleListener lifecycleListener : lifecycleListeners) {
            lifecycleListener.stateChanged(lifecycleEvent);
        }
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

    public void merge() {
        // TODO: @mm - improve cluster merge process
        synchronized (lifecycleLock) {
            fireLifecycleEvent(MERGING);
            final Node node = instance.node;
            final NodeEngineImpl nodeEngine = node.nodeEngine;
            final Collection<SplitBrainHandlerService> services = nodeEngine.getServices(SplitBrainHandlerService.class);
            final Collection<Runnable> tasks = new LinkedList<Runnable>();
            for (SplitBrainHandlerService service : services) {
                final Runnable runnable = service.prepareMergeRunnable();
                if (runnable != null) {
                    tasks.add(runnable);
                }
            }
            node.onRestart();
            node.connectionManager.restart();
            node.clusterService.onRestart();
            node.partitionService.onRestart();
            node.rejoin();
            final Collection<Future> futures = new LinkedList<Future>();
            for (Runnable task : tasks) {
                Future f = nodeEngine.getExecutionService().submit("hz:system", task);
                futures.add(f);
            }
            for (Future f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    getLogger().log(Level.SEVERE, "While merging..." , e);
                }
            }
            fireLifecycleEvent(MERGED);
        }
    }
}
