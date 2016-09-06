package com.hazelcast.cardinality.hyperloglog.struct;

abstract class AbstractHyperLogLog implements IHyperLogLog {

    private static final int LOWER_P_BOUND = 4;
    private static final int UPPER_P_BOUND = 16;

    private final IHyperLogLogContext ctx;

    // Precision
    final int p;
    final int m;

    AbstractHyperLogLog(IHyperLogLogContext ctx, int p) {
        if (p < LOWER_P_BOUND || p > UPPER_P_BOUND)
            throw new IllegalArgumentException("Precision (p) outside valid range [4..16].");

        this.ctx = ctx;
        this.p = p;
        this.m = 1 << p;
    }

    @Override
    public boolean aggregate(long[] hashes) {
        boolean changed = false;
        for (long hash : hashes)
            changed |= aggregate(hash);

        return changed;
    }

    public long linearCounting(final int m, final int numOfEmptyRegs) {
        return (long) (m * Math.log(m / (double) numOfEmptyRegs));
    }

    IHyperLogLogContext getContext() {
        return ctx;
    }

    void switchStore(final IHyperLogLog store) {
        ctx.setStore(store);
    }
}

