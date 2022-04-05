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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.impl.AbstractCacheSyncListenerCompleter;
import com.hazelcast.cache.impl.CacheSyncListenerCompleter;
import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static java.lang.Thread.currentThread;

/**
 * Implementation of {@link CacheSyncListenerCompleter} for usage inside {@link ClientCacheProxySupport}. Encapsulates management
 * of countdown latches as well as cache entry listener registrations. Supposed to be created for each {@link ClientCacheProxy}
 * instance.
 */
class ClientCacheProxySyncListenerCompleter extends AbstractCacheSyncListenerCompleter {

    private static final long MAX_COMPLETION_LATCH_WAIT_TIME = TimeUnit.MINUTES.toMillis(5);
    private static final long COMPLETION_LATCH_WAIT_TIME_STEP = TimeUnit.SECONDS.toMillis(1);

    private final ClientCacheProxySupport clientCacheProxy;

    ClientCacheProxySyncListenerCompleter(ClientCacheProxySupport clientCacheProxy) {
        this.clientCacheProxy = clientCacheProxy;
    }

    @Override
    protected void awaitLatch(CountDownLatch countDownLatch, InternalCompletableFuture future)
            throws ExecutionException {
        try {
            long currentTimeoutMs = MAX_COMPLETION_LATCH_WAIT_TIME;
            // Call latch await in small steps to be able to check if node is still active.
            // If not active then throw HazelcastInstanceNotActiveException,
            // If closed or destroyed then throw IllegalStateException,
            // otherwise continue to wait until `MAX_COMPLETION_LATCH_WAIT_TIME` passes.
            //
            // Warning: Silently ignoring if latch does not countdown in time.
            while (currentTimeoutMs > 0
                    && !countDownLatch.await(COMPLETION_LATCH_WAIT_TIME_STEP, TimeUnit.MILLISECONDS)) {
                if (future != null && future.isDone()) {
                    future.get();
                }
                currentTimeoutMs -= COMPLETION_LATCH_WAIT_TIME_STEP;
                if (!clientCacheProxy.getContext().isActive()) {
                    throw new HazelcastClientNotActiveException();
                } else if (clientCacheProxy.isClosed()) {
                    throw new IllegalStateException("Cache (" + clientCacheProxy.getPrefixedName() + ") is closed!");
                } else if (clientCacheProxy.isDestroyed()) {
                    throw new IllegalStateException("Cache (" + clientCacheProxy.getPrefixedName() + ") is destroyed!");
                }
            }
            if (countDownLatch.getCount() > 0) {
                clientCacheProxy.getLogger().finest("Countdown latch wait timeout after " + MAX_COMPLETION_LATCH_WAIT_TIME
                        + " milliseconds!");
            }
        } catch (InterruptedException e) {
            currentThread().interrupt();
            sneakyThrow(e);
        }
    }
}
