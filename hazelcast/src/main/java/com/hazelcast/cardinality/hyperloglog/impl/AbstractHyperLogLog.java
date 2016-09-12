/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cardinality.hyperloglog.impl;

import com.hazelcast.cardinality.hyperloglog.IHyperLogLog;
import com.hazelcast.cardinality.hyperloglog.IHyperLogLogCompositeContext;

abstract class AbstractHyperLogLog implements IHyperLogLog {

    private static final int LOWER_P_BOUND = 4;
    private static final int UPPER_P_BOUND = 16;

    private final IHyperLogLogCompositeContext ctx;

    // Precision
    final int p;
    final int m;

    private Long cachedEstimate;

    AbstractHyperLogLog(IHyperLogLogCompositeContext ctx, int p) {
        if (p < LOWER_P_BOUND || p > UPPER_P_BOUND) {
            throw new IllegalArgumentException("Precision (p) outside valid range [4..16].");
        }

        this.ctx = ctx;
        this.p = p;
        this.m = 1 << p;
    }

    @Override
    public boolean aggregateAll(long[] hashes) {
        boolean changed = false;
        for (long hash : hashes) {
            changed |= aggregate(hash);
        }

        return changed;
    }

    public long linearCounting(final int m, final int numOfEmptyRegs) {
        return (long) (m * Math.log(m / (double) numOfEmptyRegs));
    }

    IHyperLogLogCompositeContext getContext() {
        return ctx;
    }

    void switchStore(final IHyperLogLog store) {
        ctx.setStore(store);
    }

    Long getCachedEstimate() {
        return cachedEstimate;
    }

    long cacheAndGetLastEstimate(final Long estimate) {
        this.cachedEstimate = estimate;
        return estimate;
    }

    void invalidateCachedEstimate() {
        cachedEstimate = null;
    }
}

