package com.hazelcast.internal.probing;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.util.SetUtil.createHashSet;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Set;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.probing.ProbeRegistry.ProbeSource;

final class GcProbeSource implements ProbeSource {

    private static final Set<String> YOUNG_GC;
    private static final Set<String> OLD_GC;

    static {
        final Set<String> youngGC = createHashSet(3);
        youngGC.add("PS Scavenge");
        youngGC.add("ParNew");
        youngGC.add("G1 Young Generation");
        YOUNG_GC = Collections.unmodifiableSet(youngGC);

        final Set<String> oldGC = createHashSet(3);
        oldGC.add("PS MarkSweep");
        oldGC.add("ConcurrentMarkSweep");
        oldGC.add("G1 Old Generation");
        OLD_GC = Collections.unmodifiableSet(oldGC);
    }

    @Probe(level = MANDATORY)
    volatile long minorCount;
    @Probe(level = MANDATORY)
    volatile long minorTime;
    @Probe(level = MANDATORY)
    volatile long majorCount;
    @Probe(level = MANDATORY)
    volatile long majorTime;
    @Probe(level = MANDATORY)
    volatile long unknownCount;
    @Probe(level = MANDATORY)
    volatile long unknownTime;

    @Override
    public void probeIn(ProbingCycle cycle) {
        cycle.openContext();
        cycle.probe("gc", this);
    }

    @ReprobeCycle(4)
    public void update() {
        long minorCount = 0;
        long minorTime = 0;
        long majorCount = 0;
        long majorTime = 0;
        long unknownCount = 0;
        long unknownTime = 0;

        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            long count = gc.getCollectionCount();
            if (count == -1) {
                continue;
            }

            if (YOUNG_GC.contains(gc.getName())) {
                minorCount += count;
                minorTime += gc.getCollectionTime();
            } else if (OLD_GC.contains(gc.getName())) {
                majorCount += count;
                majorTime += gc.getCollectionTime();
            } else {
                unknownCount += count;
                unknownTime += gc.getCollectionTime();
            }
        }

        this.minorCount = minorCount;
        this.minorTime = minorTime;
        this.majorCount = majorCount;
        this.majorTime = majorTime;
        this.unknownCount = unknownCount;
        this.unknownTime = unknownTime;
    }
}