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

package com.hazelcast.cache.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.util.ExceptionUtil;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;
import static java.lang.Thread.currentThread;

/**
 * Implementation of {@link CacheSyncListenerCompleter} for usage inside {@link CacheProxySupport}. Encapsulates management of
 * countdown latches as well as cache entry listener registrations. Supposed to be created for each {@link CacheProxy} instance.
 */
class CacheProxySyncListenerCompleter
        implements CacheSyncListenerCompleter {

    private static final long MAX_COMPLETION_LATCH_WAIT_TIME = TimeUnit.MINUTES.toMillis(5);
    private static final long COMPLETION_LATCH_WAIT_TIME_STEP = TimeUnit.SECONDS.toMillis(1);

    private final AtomicInteger completionIdCounter = new AtomicInteger();
    private final ConcurrentMap<Integer, CountDownLatch> syncLocks = new ConcurrentHashMap<>();

    private final ConcurrentMap<CacheEntryListenerConfiguration, String> asyncListenerRegistrations = new ConcurrentHashMap<>();
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> syncListenerRegistrations = new ConcurrentHashMap<>();

    private final CacheProxySupport cacheProxy;

    CacheProxySyncListenerCompleter(CacheProxySupport cacheProxy) {
        this.cacheProxy = cacheProxy;
    }

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

    void deregisterCompletionLatch(Integer countDownLatchId) {
        if (countDownLatchId != IGNORE_COMPLETION) {
            syncLocks.remove(countDownLatchId);
        }
    }

    Integer registerCompletionLatch(int count) {
        if (!syncListenerRegistrations.isEmpty()) {
            int id = completionIdCounter.incrementAndGet();
            int size = syncListenerRegistrations.size();
            CountDownLatch countDownLatch = new CountDownLatch(count * size);
            syncLocks.put(id, countDownLatch);
            return id;
        }
        return IGNORE_COMPLETION;
    }

    void waitCompletionLatch(Integer countDownLatchId) {
        if (countDownLatchId != IGNORE_COMPLETION) {
            CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
            if (countDownLatch != null) {
                awaitLatch(countDownLatch);
            }
        }
    }

    void waitCompletionLatch(Integer countDownLatchId, int offset) {
        if (countDownLatchId != IGNORE_COMPLETION) {
            CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
            if (countDownLatch != null) {
                for (int i = 0; i < offset; i++) {
                    countDownLatch.countDown();
                }
                awaitLatch(countDownLatch);
            }
        }
    }

    private void awaitLatch(CountDownLatch countDownLatch) {
        try {
            long currentTimeoutMs = MAX_COMPLETION_LATCH_WAIT_TIME;
            // Call latch await in small steps to be able to check if node is still active.
            // If not active then throw HazelcastInstanceNotActiveException,
            // If closed or destroyed then throw IllegalStateException,
            // otherwise continue to wait until `MAX_COMPLETION_LATCH_WAIT_TIME` passes.
            //
            // Warning: Silently ignoring if latch does not countDown in time.
            while (currentTimeoutMs > 0 && !countDownLatch.await(COMPLETION_LATCH_WAIT_TIME_STEP, TimeUnit.MILLISECONDS)) {
                currentTimeoutMs -= COMPLETION_LATCH_WAIT_TIME_STEP;
                if (!cacheProxy.getNodeEngine().isRunning()) {
                    throw new HazelcastInstanceNotActiveException();
                } else if (cacheProxy.isClosed()) {
                    throw new IllegalStateException("Cache (" + cacheProxy.getPrefixedName() + ") is closed!");
                } else if (cacheProxy.isDestroyed()) {
                    throw new IllegalStateException("Cache (" + cacheProxy.getPrefixedName() + ") is destroyed!");
                }

            }
        } catch (InterruptedException e) {
            currentThread().interrupt();
            ExceptionUtil.sneakyThrow(e);
        }
    }

    void putListenerIfAbsent(CacheEntryListenerConfiguration configuration, String regId) {
        if (configuration.isSynchronous()) {
            syncListenerRegistrations.putIfAbsent(configuration, regId);
        } else {
            asyncListenerRegistrations.putIfAbsent(configuration, regId);
        }
    }

    void removeListener(CacheEntryListenerConfiguration configuration) {
        if (configuration.isSynchronous()) {
            syncListenerRegistrations.remove(configuration);
        } else {
            asyncListenerRegistrations.remove(configuration);
        }
    }

    String getListenerId(CacheEntryListenerConfiguration configuration) {
        if (configuration.isSynchronous()) {
            return syncListenerRegistrations.get(configuration);
        } else {
            return asyncListenerRegistrations.get(configuration);
        }
    }

    void clearListeners() {
        syncListenerRegistrations.clear();
        asyncListenerRegistrations.clear();
        notifyAndClearSyncListenerLatches();
    }

    Collection<String> getListenersIds(boolean sync) {
        if (sync) {
            return syncListenerRegistrations.values();
        } else {
            return asyncListenerRegistrations.values();
        }
    }
}
