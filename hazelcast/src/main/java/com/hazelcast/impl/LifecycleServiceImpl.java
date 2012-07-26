/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.impl.executor.ParallelExecutor;
import com.hazelcast.logging.ILogger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.*;

public class LifecycleServiceImpl implements LifecycleService {
    final FactoryImpl factory;
    final AtomicBoolean paused = new AtomicBoolean(false);
    final CopyOnWriteArrayList<LifecycleListener> lsLifecycleListeners = new CopyOnWriteArrayList<LifecycleListener>();
    final Object lifecycleLock = new Object();

    public LifecycleServiceImpl(FactoryImpl factory) {
        this.factory = factory;
    }

    private ILogger getLogger() {
        return factory.node.getLogger(LifecycleServiceImpl.class.getName());
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
        getLogger().log(Level.INFO, factory.node.getThisAddress() + " is " + lifecycleEvent.getState());
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

    public boolean isRunning() {
        synchronized (lifecycleLock) {
            return factory.node.isActive();
        }
    }

    public void shutdown() {
        synchronized (lifecycleLock) {
            fireLifecycleEvent(SHUTTING_DOWN);
            FactoryImpl.shutdown(factory);
            fireLifecycleEvent(SHUTDOWN);
        }
    }

    public void kill() {
        synchronized (lifecycleLock) {
            fireLifecycleEvent(SHUTTING_DOWN);
            FactoryImpl.kill(factory);
            fireLifecycleEvent(SHUTDOWN);
        }
    }

    public void restart() {
        synchronized (lifecycleLock) {
            fireLifecycleEvent(RESTARTING);
            paused.set(true);
            final Node node = factory.node;
            final ILogger logger = getLogger();
            List<Record> lsOwnedRecords = new ArrayList<Record>();
            for (CMap cmap : node.concurrentMapManager.getCMaps().values()) {
                if (cmap.isUserMap()) {
                    lsOwnedRecords.addAll(cmap.getMapIndexService().getOwnedRecords());
                }
            }
            node.onRestart();
            node.clientHandlerService.restart();
            node.connectionManager.onRestart();
            node.clusterImpl.onRestart();
            node.concurrentMapManager.onRestart();
            node.rejoin();
            final CountDownLatch latch = new CountDownLatch(lsOwnedRecords.size());
            final ParallelExecutor executor = node.executorManager.newParallelExecutor(16);
            for (final Record ownedRecord : lsOwnedRecords) {
                executor.execute(new Runnable() {
                    public void run() {
                        try {
                            ConcurrentMapManager.MPut mput = node.concurrentMapManager.new MPut();
                            mput.merge(ownedRecord);
                            // invalidate record now (skipped invalidation on restart)
                            ownedRecord.invalidate();
                            latch.countDown();
                        } catch (Exception e) {
                            logger.log(Level.WARNING, e.getMessage(), e);
                        }
                    }
                });
            }
            try {
                latch.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
            paused.set(false);
            fireLifecycleEvent(RESTARTED);
        }
    }
}
