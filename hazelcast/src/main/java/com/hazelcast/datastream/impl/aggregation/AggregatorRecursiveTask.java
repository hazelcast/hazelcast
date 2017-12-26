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

package com.hazelcast.datastream.impl.aggregation;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.internal.commitlog.Region;
import com.hazelcast.datastream.impl.RegionRun;
import com.hazelcast.util.function.Supplier;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

public class AggregatorRecursiveTask extends RecursiveTask<Aggregator> {

    private final Region region;
    private final Supplier<RegionRun<Aggregator>> runSupplier;
    private final CompletableFuture<Aggregator> f;

    public AggregatorRecursiveTask(CompletableFuture<Aggregator> f,
                                   Region region,
                                   Supplier<RegionRun<Aggregator>> runSupplier) {
        this.f = f;
        this.region = region;
        this.runSupplier = runSupplier;
    }

    @Override
    protected Aggregator compute() {
        Aggregator result = compute0();
        if (f != null) {
            f.complete(result);
        }
        return result;
    }

    private Aggregator compute0() {
        RegionRun<Aggregator> run = runSupplier.get();

        if (region == null) {
            return run.result();
        }

        Region previous = region.previous;
        ForkJoinTask<Aggregator> fork = null;
        if (previous != null) {
            fork = new AggregatorRecursiveTask(null, previous, runSupplier).fork();
        }

        if (!region.acquire()) {
            return fork == null ? run.result() : fork.join();
        }

        try {
            run.runSingleFullScan(region);

            Aggregator result = run.result();

            if (fork != null) {
                result.combine(fork.join());
            }

            return result;
        } finally {
            region.release();
        }
    }
}
