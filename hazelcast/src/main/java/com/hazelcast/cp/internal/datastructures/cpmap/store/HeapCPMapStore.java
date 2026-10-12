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

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cp.internal.datastructures.cpmap.store.CPMapMetricDescriptorConstants.SIZE;
import static com.hazelcast.cp.internal.datastructures.cpmap.store.CPMapMetricDescriptorConstants.SIZE_BYTES;

public class HeapCPMapStore implements CPMapStore {

    // this heap memory store should not be used anywhere near [MAX_MB], it is here to simply guard against
    // MB -> byte size translation that exceeds what [int] can store.
    public static final int LIMIT_MAX_CAPACITY_MB = 2_000;
    static final int BYTES_IN_MB = 1_000_000;

    protected final Map<Data, Data> map;
    protected final ILogger logger;
    /*
     * - The [usedDataBytes] of the store -- we rely on
     * [Data#dataSize()] and manually do the mutations
     *
     * - Note that the `usedDataBytes` field is written by a single
     * thread but read by metrics system threads. We use `AtomicInteger`
     * to ensure visibility across threads for this reason.
     */
    protected final AtomicInteger usedDataBytes;

    protected volatile int size;

    // package-private for testing needs
    long capacityInBytes;

    public HeapCPMapStore(int capacityInMb) {
        this.capacityInBytes = checkedCapacityBytes(capacityInMb);
        this.map = new LinkedHashMap<>();
        this.usedDataBytes = new AtomicInteger();
        this.logger = Logger.getLogger(getClass());
    }

    private static long checkedCapacityBytes(int capacityInMb) {
        if (capacityInMb < 1) {
            throw new IllegalArgumentException(
                    "'capacityInMb' must be >= 1 MB, got: " + capacityInMb + " MB"
            );
        }
        if (capacityInMb > LIMIT_MAX_CAPACITY_MB) {
            throw new IllegalArgumentException(
                    "'capacityInMb' must be <= " + LIMIT_MAX_CAPACITY_MB + " MB, got: " + capacityInMb + " MB"
            );
        }
        return capacityInMb * BYTES_IN_MB;
    }

    @Override
    public Iterator<Map.Entry<Data, Data>> iterator() {
        return map.entrySet().iterator();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Data get(@Nonnull Data key) {
        return map.get(key);
    }

    @Override
    public Data put(@Nonnull Data key, @Nonnull Data value, long timestamp) {
        checkWithinMaximumPermittedBytes(key, value);

        Data result = map.put(key, value);
        int unpublishedUsedDataBytes = usedDataBytes.get();
        if (result != null) {
            unpublishedUsedDataBytes -= keyValueSize(key, result);
        }
        unpublishedUsedDataBytes += keyValueSize(key, value);
        usedDataBytes.set(unpublishedUsedDataBytes);
        updateSizeMetrics();
        return result;
    }

    @Override
    public Data putIfAbsent(@Nonnull Data key, @Nonnull Data value, long timestamp) {
        Data currentValue = map.get(key);
        if (currentValue == null) {
            put(key, value, timestamp);
        }
        return currentValue;
    }

    @Override
    public Data remove(@Nonnull Data key) {
        Data value = map.remove(key);
        updateSizeMetrics();

        if (value != null) {
            int unpublishedDataBytes = usedDataBytes.get();
            unpublishedDataBytes -= keyValueSize(key, value);
            usedDataBytes.set(unpublishedDataBytes);
        }
        return value;
    }

    @Override
    public boolean compareAndSet(@Nonnull Data key, @Nonnull Data expectedValue,
                                 @Nonnull Data newValue, long timestamp) {
        Data observedValue = map.get(key);
        boolean updated = false;
        if (Objects.equals(observedValue, expectedValue)) {
            checkWithinMaximumPermittedBytes(key, newValue);
            map.put(key, newValue);
            updateSizeMetrics();
            int unpublishedUsedDataBytes = usedDataBytes.get();
            unpublishedUsedDataBytes -= keyValueSize(key, observedValue);
            unpublishedUsedDataBytes += keyValueSize(key, newValue);
            usedDataBytes.set(unpublishedUsedDataBytes);
            updated = true;
        }

        return updated;
    }

    protected void updateSizeMetrics() {
        size = map.size();
    }

    // To keep things simple we pessimistically assume that we're asking if adding the [key] and [value] data size
    // will exceed the max bytes permitted. This will result in some false positives but for the most part it should
    // be sufficient. It can be made accurate by introducing locking, which is avoided for now.
    private void checkWithinMaximumPermittedBytes(Data key, Data value) {
        int requested = keyValueSize(key, value) + metadataCost();
        int inUse = usedDataBytes.get();

        if (logger.isFinestEnabled()) {
            String logg = String.format("Requested: %d bytes (%.2f MB), "
                            + "In use: %d bytes (%.2f MB), Capacity: %d bytes (%d MB)",
                    requested,
                    requested / (double) BYTES_IN_MB,
                    inUse,
                    inUse / (double) BYTES_IN_MB,
                    capacityInBytes,
                    capacityInBytes / BYTES_IN_MB);
            logger.finest(logg);
        }

        if ((requested + inUse) > capacityInBytes) {
            throw new IllegalStateException(
                    "Write not permitted as it would exceed the user defined capacity limit of "
                            + (capacityInBytes / BYTES_IN_MB) + "MB");
        }
    }

    protected int metadataCost() {
        return 0;
    }

    protected static int keyValueSize(Data key, Data value) {
        int keySize = key == null ? 0 : key.dataSize();
        int valueSize = value == null ? 0 : value.dataSize();
        return keySize + valueSize;
    }

    @Override
    public void collectMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        context.collect(descriptor.copy().withMetric(SIZE), size());
        context.collect(descriptor.copy().withMetric(SIZE_BYTES), usedDataBytes.get());
    }

    int getUsedDataBytes() {
        return usedDataBytes.get();
    }
}
