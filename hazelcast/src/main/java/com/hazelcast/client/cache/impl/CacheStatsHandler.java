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

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.Timer;

import java.util.function.BiConsumer;

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
            statistics.addGetTimeNanos(Timer.nanosElapsed(startNanos));
            if (response != null) {
                statistics.increaseCacheHits();
                statistics.increaseCachePuts();
                statistics.addPutTimeNanos(Timer.nanosElapsed(startNanos));
            } else {
                statistics.increaseCacheMisses();
            }
        } else {
            if (Boolean.TRUE.equals(response)) {
                statistics.increaseCacheHits();
                statistics.increaseCachePuts();
                statistics.addPutTimeNanos(Timer.nanosElapsed(startNanos));
            } else {
                statistics.increaseCacheMisses();
            }
        }
    }

    <T> BiConsumer<T, Throwable> newOnReplaceCallback(final long startNanos) {
        return (response, throwable) -> {
            if (throwable == null) {
                onReplace(true, startNanos, toObject(response));
            }
        };
    }

    void onPutIfAbsent(long startNanos, boolean saved) {
        if (saved) {
            statistics.increaseCachePuts();
            statistics.increaseCacheMisses();
            statistics.addPutTimeNanos(Timer.nanosElapsed(startNanos));
        }
    }

    BiConsumer<Boolean, Throwable> newOnPutIfAbsentCallback(final long startNanos) {
        return (responseData, throwable) -> {
            if (throwable == null) {
                Object response = toObject(responseData);
                onPutIfAbsent(startNanos, (Boolean) response);
            }
        };
    }

    void onPut(boolean isGet, long startNanos, boolean cacheHit) {
        statistics.increaseCachePuts();
        statistics.addPutTimeNanos(Timer.nanosElapsed(startNanos));
        if (isGet) {
            statistics.addGetTimeNanos(Timer.nanosElapsed(startNanos));
            if (cacheHit) {
                statistics.increaseCacheHits();
            } else {
                statistics.increaseCacheMisses();
            }
        }
    }

    <T> BiConsumer<T, Throwable> newOnPutCallback(final boolean isGet, final long startNanos) {
        return (responseData, throwable) -> {
            if (throwable == null) {
                onPut(isGet, startNanos, responseData != null);
            }
        };
    }

    void onRemove(boolean isGet, long startNanos, Object response) {
        if (isGet) {
            statistics.addGetTimeNanos(Timer.nanosElapsed(startNanos));
            if (response != null) {
                statistics.increaseCacheHits();
                statistics.increaseCacheRemovals();
                statistics.addRemoveTimeNanos(Timer.nanosElapsed(startNanos));
            } else {
                statistics.increaseCacheMisses();
            }
        } else {
            if (Boolean.TRUE.equals(toObject(response))) {
                statistics.increaseCacheRemovals();
                statistics.addRemoveTimeNanos(Timer.nanosElapsed(startNanos));
            }
        }
    }

    <T> BiConsumer<T, Throwable> newOnRemoveCallback(final boolean isGet, final long startNanos) {
        return (v, throwable) -> {
            if (throwable == null) {
                onRemove(isGet, startNanos, v);
            }
        };
    }

    void onGet(long startNanos, boolean responseReceived) {
        if (responseReceived) {
            statistics.increaseCacheHits();
        } else {
            statistics.increaseCacheMisses();
        }
        statistics.addGetTimeNanos(Timer.nanosElapsed(startNanos));
    }

    <T> BiConsumer<T, Throwable> newOnGetCallback(final long startNanos) {
        return (response, throwable) -> {
            if (throwable == null) {
                onGet(startNanos, response != null);
            }
        };
    }

    void onBatchRemove(long startNanos, int batchSize) {
        // Actually we don't know how many of them are really removed or not.
        // We just assume that if there is no exception, all of them are removed.
        // Otherwise (if there is an exception), we don't update any cache stats about remove.
        statistics.increaseCacheRemovals(batchSize);
        statistics.addRemoveTimeNanos(Timer.nanosElapsed(startNanos));
    }

    void onBatchGet(long startNanos, int batchSize) {
        statistics.increaseCacheHits(batchSize);
        statistics.addGetTimeNanos(Timer.nanosElapsed(startNanos));
    }

    void onBatchPut(long startNanos, int batchSize) {
        // we don't know how many of keys are actually loaded, so we assume that all of them are loaded
        // and calculate statistics based on this assumption
        statistics.increaseCachePuts(batchSize);
        statistics.addPutTimeNanos(Timer.nanosElapsed(startNanos));
    }

    void clear() {
        statistics.clear();
    }

    private Object toObject(Object responseData) {
        return serializationService.toObject(responseData);
    }
}
