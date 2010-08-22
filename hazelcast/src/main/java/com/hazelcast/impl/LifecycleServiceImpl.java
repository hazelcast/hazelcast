/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.core.LifecycleEvent;
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

public class LifecycleServiceImpl implements LifecycleService {
    final FactoryImpl factory;
    final Node node;
    final ILogger logger;

    final AtomicBoolean paused = new AtomicBoolean(false);

    final CopyOnWriteArrayList<LifecycleListener> lsLifecycleListeners = new CopyOnWriteArrayList<LifecycleListener>();

    final Object lifecycleLock = new Object();

    public LifecycleServiceImpl(FactoryImpl factory) {
        this.factory = factory;
        this.node = factory.node;
        logger = node.getLogger(LifecycleServiceImpl.class.getName());
    }

    public void addLifecycleListener(LifecycleListener lifecycleListener) {
        lsLifecycleListeners.add(lifecycleListener);
    }

    public void removeLifecycleListener(LifecycleListener lifecycleListener) {
        lsLifecycleListeners.remove(lifecycleListener);
    }

    public void fireLifecycleEvent(LifecycleEvent lifecycleEvent) {
        logger.log(Level.INFO, node.getThisAddress() + " lifecycle event " + lifecycleEvent.getState());
        for (LifecycleListener lifecycleListener : lsLifecycleListeners) {
            lifecycleListener.stateChanged(lifecycleEvent);
        }
    }

    public boolean pause() {
        synchronized (lifecycleLock) {
            if (!paused.get()) {
                fireLifecycleEvent(new LifecycleEvent(LifecycleEvent.LifecycleState.PAUSING));
            } else {
                return false;
            }
            paused.set(true);
            fireLifecycleEvent(new LifecycleEvent(LifecycleEvent.LifecycleState.PAUSED));
            return true;
        }
    }

    public boolean resume() {
        synchronized (lifecycleLock) {
            if (paused.get()) {
                fireLifecycleEvent(new LifecycleEvent(LifecycleEvent.LifecycleState.RESTARTING));
            } else {
                return false;
            }
            paused.set(false);
            fireLifecycleEvent(new LifecycleEvent(LifecycleEvent.LifecycleState.RESTARTED));
            return true;
        }
    }

    public boolean isRunning() {
        synchronized (lifecycleLock) {
            return node.isActive();
        }
    }

    public void shutdown() {
        synchronized (lifecycleLock) {
            fireLifecycleEvent(new LifecycleEvent(LifecycleEvent.LifecycleState.SHUTTING_DOWN));
            FactoryImpl.shutdown(factory.getHazelcastInstanceProxy());
            fireLifecycleEvent(new LifecycleEvent(LifecycleEvent.LifecycleState.SHUTDOWN));
        }
    }

    public void restart() {
        synchronized (lifecycleLock) {
            fireLifecycleEvent(new LifecycleEvent(LifecycleEvent.LifecycleState.RESTARTING));
            paused.set(true);
            List<Record> lsOwnedRecords = new ArrayList<Record>();
            for (CMap cmap : node.concurrentMapManager.getCMaps().values()) {
                if (cmap.isUserMap()) {
                    lsOwnedRecords.addAll(cmap.getMapIndexService().getOwnedRecords());
                }
            }
            node.connectionManager.onRestart();
            node.clusterManager.onRestart();
            node.concurrentMapManager.onRestart();
            node.rejoin();
            final CountDownLatch latch = new CountDownLatch(lsOwnedRecords.size());
            final ParallelExecutor executor = node.executorManager.newParallelExecutor(16);
            for (final Record ownedRecord : lsOwnedRecords) {
                executor.execute(new Runnable() {
                    public void run() {
                        try {
                            ConcurrentMapManager.MPut mput = node.concurrentMapManager.new MPut();
                            System.out.println("sending merge");
                            mput.merge(ownedRecord);
                            latch.countDown();
                            System.out.println("merge complete " + latch.getCount());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
            try {
                latch.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
            logger.log(Level.INFO, node.getThisAddress() + " is restarted!");
            paused.set(false);
            fireLifecycleEvent(new LifecycleEvent(LifecycleEvent.LifecycleState.RESTARTED));
        }
    }
}
