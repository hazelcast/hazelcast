package com.hazelcast.internal.diagnostics;

import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public final class Distribution {

    private static final AtomicLongFieldUpdater<Distribution> COUNT
            = newUpdater(Distribution.class, "count");
    private static final AtomicLongFieldUpdater<Distribution> TOTAL
            = newUpdater(Distribution.class, "total");
    private static final AtomicLongFieldUpdater<Distribution> MAX
            = newUpdater(Distribution.class, "max");

    @Probe
    volatile long count;
    @Probe
    volatile long max;
    @Probe
    volatile long total;

    final AtomicLongArray buckets;
    private final int divider;

    public Distribution(int buckets, int divider) {
        this.buckets = new AtomicLongArray(buckets);
        this.divider = divider;

    }

    public void provideMetrics(String name, MetricsRegistry registry) {
        registry.scanAndRegister(this, name);
        for (int k = 0; k < buckets.length(); k++) {
            final int index = k;
            registry.register(buckets, name + "[" + k + "]", INFO, new LongProbeFunction<AtomicLongArray>() {
                @Override
                public long get(AtomicLongArray source) {
                    return source.get(index);
                }
            });
        }
    }

    public void record(long original) {
        long v = original / divider;

        if (v < 0) {
            throw new IllegalArgumentException();
        }

        COUNT.addAndGet(this, 1);
        TOTAL.addAndGet(this, v);

        updateMax(v);

        int bucketIndex = toIndex(v);
        //if(count %100==0)
        System.out.println("index:"+bucketIndex);
        buckets.incrementAndGet(bucketIndex);
    }

    private void updateMax(long v) {
        for (; ; ) {
            long currentMax = max;
            if (v <= currentMax) {
                break;
            }

            if (MAX.compareAndSet(this, currentMax, v)) {
                break;
            }
        }
    }

    private int toIndex(long v) {
        return 64-Long.numberOfLeadingZeros(v);
    }

    public long max() {
        return max;
    }

    public long total() {
        return total;
    }
}
