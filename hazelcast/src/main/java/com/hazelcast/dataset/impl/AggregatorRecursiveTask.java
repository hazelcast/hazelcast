package com.hazelcast.dataset.impl;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.util.function.Supplier;

import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

public class AggregatorRecursiveTask extends RecursiveTask<Aggregator> {

    private final Segment segment;
    private final Supplier<SegmentRun<Aggregator>> runSupplier;

    public AggregatorRecursiveTask(Segment segment, Supplier<SegmentRun<Aggregator>> runSupplier) {
        this.segment = segment;
        this.runSupplier = runSupplier;
    }

    @Override
    protected Aggregator compute() {
        SegmentRun<Aggregator> run = runSupplier.get();

        if (segment == null) {
            return run.result();
        }

        Segment previous = segment.previous;
        ForkJoinTask<Aggregator> fork = null;
        if (previous != null) {
            fork = new AggregatorRecursiveTask(previous, runSupplier).fork();
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
