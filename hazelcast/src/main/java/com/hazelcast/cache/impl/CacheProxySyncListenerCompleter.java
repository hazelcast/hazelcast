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

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.currentThread;

/**
 * Implementation of {@link CacheSyncListenerCompleter} for usage inside {@link CacheProxySupport}. Encapsulates management of
 * countdown latches as well as cache entry listener registrations. Supposed to be created for each {@link CacheProxy} instance.
 */
class CacheProxySyncListenerCompleter extends AbstractCacheSyncListenerCompleter {

    private static final long MAX_COMPLETION_LATCH_WAIT_TIME = TimeUnit.MINUTES.toMillis(5);
    private static final long COMPLETION_LATCH_WAIT_TIME_STEP = TimeUnit.SECONDS.toMillis(1);

    private final CacheProxySupport cacheProxy;

    CacheProxySyncListenerCompleter(CacheProxySupport cacheProxy) {
        this.cacheProxy = cacheProxy;
    }

    @Override
    protected void awaitLatch(CountDownLatch countDownLatch, InternalCompletableFuture future) {
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
}
