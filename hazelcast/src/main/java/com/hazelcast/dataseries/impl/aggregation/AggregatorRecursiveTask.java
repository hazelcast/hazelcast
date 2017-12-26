package com.hazelcast.dataseries.impl.aggregation;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.dataseries.impl.Segment;
import com.hazelcast.dataseries.impl.SegmentRun;
import com.hazelcast.util.function.Supplier;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

public class AggregatorRecursiveTask extends RecursiveTask<Aggregator> {

    private final Segment segment;
    private final Supplier<SegmentRun<Aggregator>> runSupplier;
    private final CompletableFuture<Aggregator> f;

    public AggregatorRecursiveTask(CompletableFuture<Aggregator> f,
                                   Segment segment,
                                   Supplier<SegmentRun<Aggregator>> runSupplier) {
        this.f = f;
        this.segment = segment;
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
        SegmentRun<Aggregator> run = runSupplier.get();

        if (segment == null) {
            return run.result();
        }

        Segment previous = segment.previous;
        ForkJoinTask<Aggregator> fork = null;
        if (previous != null) {
            fork = new AggregatorRecursiveTask(null, previous, runSupplier).fork();
        }

        if (!segment.acquire()) {
            return fork == null ? run.result() : fork.join();
        }

        try {
            run.runSingleFullScan(segment);

            Aggregator result = run.result();

            if (fork != null) {
                result.combine(fork.join());
            }

            return result;
        } finally {
            segment.release();
        }
    }
}
