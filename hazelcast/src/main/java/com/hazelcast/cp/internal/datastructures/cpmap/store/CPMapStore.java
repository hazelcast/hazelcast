/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap.store;

import com.hazelcast.config.cp.CPMapConfig;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapRaftOp;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.serialization.Data;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * More-or-less the corresponding type variable erased (key-value are {@link Data}) version of {@link com.hazelcast.cp.CPMap}.
 * This is the contract that any new backing store for a {@link com.hazelcast.cp.CPMap} must adhere. The implementation does not
 * need to be thread-safe.
 * <p>
 * This is an internally used contract. Implementations should expect that arguments are always non-null. This invariant is
 * asserted at the immediate interface that the user comes into contact, i.e. the implementations of server and client
 * implementations of {@link com.hazelcast.cp.CPMap}. Nonnull annotations and javadoc serve simply as a soft reminder, not
 * adherence.
 * </p>
 */
public interface CPMapStore {

    /**
     * @see CPMapRaftOp#getLeaderTimestamp()
     */
    long NO_TIMESTAMP = -1L;

    /**
     * Associates {@code key} with {@code value}.
     *
     * @param key       non-null key
     * @param value     non-null value
     * @param timestamp timestamp of leader node when
     *                  {@link CPMapConfig#isPurgeEnabled()} is {@code true},
     *                  otherwise {@value #NO_TIMESTAMP}
     * @return previous value associated with {@code key}, otherwise null
     */
    Data put(@Nonnull Data key, @Nonnull Data value, long timestamp);

    /**
     * Removes {@code key}.
     *
     * @param key non-null key
     * @return value associated with {@code key} if present, otherwise null
     */
    Data remove(@Nonnull Data key);

    /**
     * Compares the value currently associated with {@code key} for equality with {@code expectedValue}. If the equality test
     * succeeds then {@code key} is associated with {@code newValue}.
     * <p>
     * Equality of {@link Data}s should use {@link java.util.Objects#equals(Object, Object)}.
     * </p>
     *
     * @param key           non-null key
     * @param expectedValue non-null value to test against what is currently associated with {@code key}
     * @param newValue      non-null value to install should the equality test succeed
     * @param timestamp     timestamp of leader node when
     *                      {@link CPMapConfig#isPurgeEnabled()} is {@code true},
     *                      otherwise {@value #NO_TIMESTAMP}
     * @return true if {@code key} was associated with {@code newValue}, otherwise false
     */
    boolean compareAndSet(@Nonnull Data key, @Nonnull Data expectedValue,
                          @Nonnull Data newValue, long timestamp);

    /**
     * Gets the value associated with {@code key}.
     *
     * @param key non-null key
     * @return value associated with {@code key} if present, otherwise null
     */
    Data get(@Nonnull Data key);

    /**
     * Collects the metrics to be published.
     *
     * @param descriptor the descriptor that should be cloned per-metric
     * @param context    the context to collect provided metrics
     */
    void collectMetrics(MetricDescriptor descriptor, MetricsCollectionContext context);

    /**
     * Associates {@code key} with {@code value} if {@code key} is not present.
     *
     * @param key       non-null key
     * @param value     non-null value
     * @param timestamp timestamp of leader node when
     *                  {@link CPMapConfig#isPurgeEnabled()} is {@code true},
     *                  otherwise {@value #NO_TIMESTAMP}
     * @return value associated with {@code key} if {@code key} is already present, otherwise null
     */
    Data putIfAbsent(@Nonnull Data key, @Nonnull Data value, long timestamp);

    /**
     * @return iterator instance to traverse {@link CPMapStore}
     */
    Iterator<Map.Entry<Data, Data>> iterator();

    /**
     * @return the number of key-value mappings
     * @see ConcurrentHashMap#size()
     */
    int size();

    default boolean isPurgeEnabled() {
        return false;
    }

    default Map<Data, Long> getEntryTimestamps() {
        return Collections.emptyMap();
    }

    default Object purge(long ageMillis, long timestamp) {
        throw new UnsupportedOperationException();
    }

}
