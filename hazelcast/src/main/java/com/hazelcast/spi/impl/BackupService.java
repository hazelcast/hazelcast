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

package com.hazelcast.spi.impl;

import com.hazelcast.logging.ILogger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * @mdogan 4/5/13
 */
public class BackupService {

    private final NodeEngineImpl nodeEngine;
    private final AtomicLong[] versions;
    private final ConcurrentMap<Long, Semaphore> syncBackups;
    private final ILogger logger;

    public BackupService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getGroupProperties().PARTITION_COUNT.getInteger();
        versions = new AtomicLong[partitionCount];
        for (int i = 0; i < versions.length; i++) {
            versions[i] = new AtomicLong();
        }
        final int coreSize = Runtime.getRuntime().availableProcessors();
        syncBackups = new ConcurrentHashMap<Long, Semaphore>(1000, 0.75f, (coreSize >= 8 ? coreSize * 4 : 16));
        logger = nodeEngine.getLogger(getClass());
    }

    void notifyCall(long callId) {
        final Semaphore lock = syncBackups.get(callId);
        if (lock == null) {
            logger.log(Level.WARNING, "No backup record found for call[" + callId + "]!");
        } else {
            lock.release();
        }
    }

    boolean waitFor(long callId, int backupCount, long timeout, TimeUnit unit) throws InterruptedException {
        final Semaphore lock = syncBackups.get(callId);
        if (lock == null) {
            throw new IllegalStateException("No backup record found for call -> " + callId);
        }
        try {
            return lock.tryAcquire(backupCount, timeout, unit);
        } finally {
            syncBackups.remove(callId);
        }
    }

    void registerCall(long callId) {
        final Semaphore current = syncBackups.put(callId, new Semaphore(0));
        if (current != null) {
            logger.log(Level.WARNING, "There is already a record for call[" + callId + "]!");
        }
    }

    void deregisterCall(long callId) {
        syncBackups.remove(callId);
    }

    void checkBackupVersion(int partitionId, long version) {

    }

    long incrementAndGetVersion(int partitionId) {
        return versions[partitionId].incrementAndGet();
    }
}
