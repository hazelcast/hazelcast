/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.spi.serialization.SerializationService;

/**
 * Helper class which contains specific statistic calculations per context
 */
final class CacheStatsHandler {

    private final SerializationService serializationService;
    private final ClientCacheStatisticsImpl statistics;

    CacheStatsHandler(SerializationService serializationService) {
        this.serializationService = serializationService;
        this.statistics = new ClientCacheStatisticsImpl(System.currentTimeMillis());
    }

    ClientCacheStatisticsImpl getStatistics() {
        return statistics;
    }

    void onReplace(boolean isGet, long startNanos, Object response) {
        if (isGet) {
            statistics.addGetTimeNanos(System.nanoTime() - startNanos);
            if (response != null) {
                statistics.increaseCacheHits();
                statistics.increaseCachePuts();
                statistics.addPutTimeNanos(System.nanoTime() - startNanos);
            } else {
                statistics.increaseCacheMisses();
            }
        } else {
            if (Boolean.TRUE.equals(response)) {
                statistics.increaseCacheHits();
                statistics.increaseCachePuts();
                statistics.addPutTimeNanos(System.nanoTime() - startNanos);
            } else {
                statistics.increaseCacheMisses();
            }
        }
    }

    <T> ExecutionCallback<T> newOnReplaceCallback(final long startNanos) {
        return new ExecutionCallback<T>() {
            @Override
            public void onResponse(T response) {
                onReplace(true, startNanos, toObject(response));
            }

            @Override
            public void onFailure(Throwable t) {
            }
        };
    }

    void onPutIfAbsent(long startNanos, boolean saved) {
        if (saved) {
            statistics.increaseCachePuts();
            statistics.addPutTimeNanos(System.nanoTime() - startNanos);
        }
    }

    ExecutionCallback<Boolean> newOnPutIfAbsentCallback(final long startNanos) {
        return new ExecutionCallback<Boolean>() {
            @Override
            public void onResponse(Boolean responseData) {
                Object response = toObject(responseData);
                onPutIfAbsent(startNanos, (Boolean) response);
            }

            @Override
            public void onFailure(Throwable t) {
            }
        };
    }

    void onPut(boolean isGet, long startNanos, boolean cacheHit) {
        statistics.increaseCachePuts();
        statistics.addPutTimeNanos(System.nanoTime() - startNanos);
        if (isGet) {
            statistics.addGetTimeNanos(System.nanoTime() - startNanos);
            if (cacheHit) {
                statistics.increaseCacheHits();
            } else {
                statistics.increaseCacheMisses();
            }
        }
    }

    <T> OneShotExecutionCallback<T> newOnPutCallback(final boolean isGet, final long startNanos) {
        return new OneShotExecutionCallback<T>() {
            @Override
            protected void onResponseInternal(T responseData) {
                onPut(isGet, startNanos, responseData != null);
            }

            @Override
            protected void onFailureInternal(Throwable t) {
            }
        };
    }

    void onRemove(boolean isGet, long startNanos, Object response) {
        if (isGet) {
            statistics.addGetTimeNanos(System.nanoTime() - startNanos);
            if (response != null) {
                statistics.increaseCacheHits();
                statistics.increaseCacheRemovals();
                statistics.addRemoveTimeNanos(System.nanoTime() - startNanos);
            } else {
                statistics.increaseCacheMisses();
            }
        } else {
            if (Boolean.TRUE.equals(toObject(response))) {
                statistics.increaseCacheRemovals();
                statistics.addRemoveTimeNanos(System.nanoTime() - startNanos);
            }
        }
    }

    <T> ExecutionCallback<T> newOnRemoveCallback(final boolean isGet, final long startNanos) {
        return new ExecutionCallback<T>() {
            public void onResponse(T response) {
                onRemove(isGet, startNanos, response);
            }

            public void onFailure(Throwable t) {
            }
        };
    }

    void onGet(long startNanos, boolean responseReceived) {
        if (responseReceived) {
            statistics.increaseCacheHits();
        } else {
            statistics.increaseCacheMisses();
        }
        statistics.addGetTimeNanos(System.nanoTime() - startNanos);
    }

    <T> ExecutionCallback<T> newOnGetCallback(final long startNanos) {
        return new ExecutionCallback<T>() {
            @Override
            public void onResponse(T response) {
                onGet(startNanos, response != null);
            }

            @Override
            public void onFailure(Throwable t) {
            }
        };
    }

    void onBatchRemove(long startNanos, int batchSize) {
        // Actually we don't know how many of them are really removed or not.
        // We just assume that if there is no exception, all of them are removed.
        // Otherwise (if there is an exception), we don't update any cache stats about remove.
        statistics.increaseCacheRemovals(batchSize);
        statistics.addRemoveTimeNanos(System.nanoTime() - startNanos);
    }

    void onBatchGet(long startNanos, int batchSize) {
        statistics.increaseCacheHits(batchSize);
        statistics.addGetTimeNanos(System.nanoTime() - startNanos);
    }

    void onBatchPut(long startNanos, int batchSize) {
        // we don't know how many of keys are actually loaded, so we assume that all of them are loaded
        // and calculate statistics based on this assumption
        statistics.increaseCachePuts(batchSize);
        statistics.addPutTimeNanos(System.nanoTime() - startNanos);
    }

    void clear() {
        statistics.clear();
    }

    private Object toObject(Object responseData) {
        return serializationService.toObject(responseData);
    }
}
