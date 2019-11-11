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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.MetricDescriptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;

/**
 * Pooling {@link MetricDescriptor} {@link Supplier}. Meant to be used
 * exclusively by the dynamic metrics collection. Not thread-safe.
 */
class PoolingMetricDescriptorSupplier implements Supplier<MetricDescriptorImpl> {
    static final int INITIAL_CAPACITY = 32;
    private static final double GROW_FACTOR = 1.2D;

    private final List<MetricDescriptorImpl> allCreated = new ArrayList<>(INITIAL_CAPACITY);
    private MetricDescriptorImpl[] pool = new MetricDescriptorImpl[INITIAL_CAPACITY];
    private int poolPtr;
    private boolean closed;

    PoolingMetricDescriptorSupplier() {
        for (int i = 0; i < pool.length; i++) {
            MetricDescriptorImpl descriptor = new MetricDescriptorImpl(this);
            pool[i] = descriptor;
            allCreated.add(descriptor);
        }
        poolPtr = pool.length - 1;
    }

    @Override
    public MetricDescriptorImpl get() {
        if (closed) {
            throw new IllegalStateException("This PoolingMetricDescriptorSupplier is already closed and cannot supply");
        }

        if (poolPtr >= 0) {
            MetricDescriptorImpl descriptor = pool[poolPtr];
            pool[poolPtr--] = null;
            descriptor.reset();
            return descriptor;
        }

        MetricDescriptorImpl descriptor = new MetricDescriptorImpl(this);
        allCreated.add(descriptor);
        return descriptor;
    }

    /**
     * Re-inserts the given descriptor into the pool.
     *
     * @param descriptor The descriptor to re-insert
     */
    void recycle(MetricDescriptorImpl descriptor) {
        ensureCapacity(poolPtr + 1);
        pool[++poolPtr] = descriptor;
    }

    /**
     * Releases all taken but not recycled {@link MetricDescriptorImpl}
     * instances. Used to make sure that there is no leaking is possible
     * if there is a reference stored to any of the descriptors taken from
     * this pool.
     */
    void close() {
        closed = true;
        for (MetricDescriptorImpl descriptor : allCreated) {
            descriptor.setSupplier(DEFAULT_DESCRIPTOR_SUPPLIER);
        }
        allCreated.clear();
        Arrays.fill(pool, null);
    }

    private void ensureCapacity(int poolPtr) {
        if (poolPtr < pool.length - 1) {
            return;
        }

        int newCapacity = (int) Math.ceil(poolPtr * GROW_FACTOR);
        MetricDescriptorImpl[] newPool = new MetricDescriptorImpl[newCapacity];
        System.arraycopy(pool, 0, newPool, 0, pool.length);
        pool = newPool;
    }
}
