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
import com.hazelcast.cardinality.hyperloglog.IHyperLogLogContext;

import static com.hazelcast.cardinality.hyperloglog.impl.HyperLogLogEncType.SPARSE;

public class HyperLogLogImpl
        implements IHyperLogLog, IHyperLogLogContext {

    private static final int DEFAULT_HLL_PRECISION = 14;

    private IHyperLogLog store;

    public HyperLogLogImpl() {
        this.store = SPARSE.build(this, DEFAULT_HLL_PRECISION);
    }

    @Override
    public boolean aggregate(long hash) {
        return store.aggregate(hash);
    }

    @Override
    public boolean aggregateAll(long[] hashes) {
        return store.aggregateAll(hashes);
    }

    @Override
    public long estimate() {
        return store.estimate();
    }

    @Override
    public void setStore(IHyperLogLog store) {
        this.store = store;
    }
}
