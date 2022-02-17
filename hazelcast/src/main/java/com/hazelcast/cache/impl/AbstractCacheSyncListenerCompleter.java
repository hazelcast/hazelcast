/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;

/**
 * Abstract {@link CacheSyncListenerCompleter} implementation which provides storage and management of countdown latches
 * and cache entry listener registrations.
 */
public abstract class AbstractCacheSyncListenerCompleter
        implements CacheSyncListenerCompleter {

    private final AtomicInteger completionIdCounter = new AtomicInteger();
    private final ConcurrentMap<Integer, CountDownLatch> syncLocks = new ConcurrentHashMap<>();

    private final ConcurrentMap<CacheEntryListenerConfiguration, UUID> asyncListenerRegistrations = new ConcurrentHashMap<>();
    private final ConcurrentMap<CacheEntryListenerConfiguration, UUID> syncListenerRegistrations = new ConcurrentHashMap<>();

    @Override
    public void countDownCompletionLatch(int countDownLatchId) {
        if (countDownLatchId != IGNORE_COMPLETION) {
            CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
            if (countDownLatch == null) {
                return;
            }
            countDownLatch.countDown();
            if (countDownLatch.getCount() == 0) {
                deregisterCompletionLatch(countDownLatchId);
            }
        }
    }

    private void notifyAndClearSyncListenerLatches() {
        // notify waiting sync listeners
        Collection<CountDownLatch> latches = syncLocks.values();
        Iterator<CountDownLatch> iterator = latches.iterator();
        while (iterator.hasNext()) {
            CountDownLatch latch = iterator.next();
            iterator.remove();
            while (latch.getCount() > 0) {
                latch.countDown();
            }
        }
    }

    public void deregisterCompletionLatch(Integer countDownLatchId) {
        if (countDownLatchId != IGNORE_COMPLETION) {
            syncLocks.remove(countDownLatchId);
        }
    }

    public Integer registerCompletionLatch(int count) {
        return registerCompletionLatch(completionIdCounter.incrementAndGet(), count);
    }

    public Integer registerCompletionLatch(Integer countDownLatchId, int count) {
        if (!syncListenerRegistrations.isEmpty()) {
            int size = syncListenerRegistrations.size();
            CountDownLatch countDownLatch = new CountDownLatch(count * size);
            syncLocks.put(countDownLatchId, countDownLatch);
            return countDownLatchId;
        }
        return MutableOperation.IGNORE_COMPLETION;
    }

    public void waitCompletionLatch(Integer countDownLatchId, InternalCompletableFuture future)
            throws ExecutionException {
        if (countDownLatchId != IGNORE_COMPLETION) {
            CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
            if (countDownLatch != null) {
                awaitLatch(countDownLatch, future);
            }
        }
    }

    public void waitCompletionLatch(Integer countDownLatchId)
            throws ExecutionException {
        waitCompletionLatch(countDownLatchId, null);
    }

    public void waitCompletionLatch(Integer countDownLatchId, int offset)
            throws ExecutionException {
        if (countDownLatchId != IGNORE_COMPLETION) {
            CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
            if (countDownLatch != null) {
                for (int i = 0; i < offset; i++) {
                    countDownLatch.countDown();
                }
                awaitLatch(countDownLatch, null);
            }
        }
    }

    protected abstract void awaitLatch(CountDownLatch countDownLatch, InternalCompletableFuture future)
            throws ExecutionException;

    public void putListenerIfAbsent(CacheEntryListenerConfiguration configuration, UUID regId) {
        if (configuration.isSynchronous()) {
            syncListenerRegistrations.putIfAbsent(configuration, regId);
        } else {
            asyncListenerRegistrations.putIfAbsent(configuration, regId);
        }
    }

    public UUID removeListener(CacheEntryListenerConfiguration configuration) {
        if (configuration.isSynchronous()) {
            return syncListenerRegistrations.remove(configuration);
        } else {
            return asyncListenerRegistrations.remove(configuration);
        }
    }

    public UUID getListenerId(CacheEntryListenerConfiguration configuration) {
        if (configuration.isSynchronous()) {
            return syncListenerRegistrations.get(configuration);
        } else {
            return asyncListenerRegistrations.get(configuration);
        }
    }

    public void clearListeners() {
        syncListenerRegistrations.clear();
        asyncListenerRegistrations.clear();
        notifyAndClearSyncListenerLatches();
    }

    public Collection<UUID> getListenersIds(boolean sync) {
        if (sync) {
            return syncListenerRegistrations.values();
        } else {
            return asyncListenerRegistrations.values();
        }
    }
}
