package com.hazelcast.internal.probing;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.probing.CharSequenceUtils.startsWith;
import static com.hazelcast.internal.probing.ProbeRegistry.ProbeSource.TAG_INSTANCE;
import static com.hazelcast.internal.probing.ProbeRegistry.ProbeSource.TAG_TARGET;
import static com.hazelcast.internal.probing.ProbeRegistry.ProbeSource.TAG_TYPE;
import static com.hazelcast.util.SetUtil.createHashSet;
import static java.lang.Math.round;

import java.io.File;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.probing.CharSequenceUtils.Lines;
import com.hazelcast.internal.probing.ProbeRegistry.ProbeSource;
import com.hazelcast.internal.probing.ProbingCycle.Tags;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.monitor.LocalIndexStats;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.LocalDistributedObjectStats;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;

/**
 * Provides utilities around probing.
 * 
 * This includes general conversion functionality like {@link #toLong(boolean)}
 * as well as common metrics on core types of objects that cannot be annotated.
 */
public final class Probing {

    private Probing() {
        // utility
    }

    /**
     * @param value any double value
     * @return the long value representing the double as expected by a
     *         {@link ProbeRenderer} that only works in longs
     */
    static long toLong(double value) {
        return round(value * 10000d);
    }

    /**
     * @param value any boolean value
     * @return the long representing the boolean as expected by a
     *         {@link ProbeRenderer} that only works in longs
     */
    static long toLong(boolean value) {
        return value ? 1 : 0;
    }

    /**
     * Converts instances to their long representation.
     * 
     * Supported are: Primitive wrapper types for {@link Number}s, atomic
     * {@link Number}s, {@link Counter}s, {@link Boolean} and {@link AtomicBoolean}.
     * In case of {@link Collection} and {@link Map} their size is returned.
     * 
     * @param value a value of a set of supported types
     * @return the long representing the passed object value
     */
    static long toLong(Object value) {
        if (value == null) {
            return -1L;
        }
        Class<?> type = value.getClass();
        if (value instanceof Number) {
            if (type == Float.class || type == Double.class) {
                return Probing.toLong(((Number) value).doubleValue());
            }
            return ((Number) value).longValue();
        }
        if (type == Boolean.class) {
            return Probing.toLong(((Boolean) value).booleanValue());
        }
        if (type == AtomicBoolean.class) {
            return Probing.toLong(((AtomicBoolean) value).get());
        }
        if (value instanceof Collection) {
            return ((Collection<?>) value).size();
        }
        if (value instanceof Map) {
            return ((Map<?, ?>) value).size();
        }
        if (value instanceof Counter) {
            return ((Counter) value).get();
        }
        if (value instanceof Semaphore) {
            return ((Semaphore) value).availablePermits();
        }
        throw new UnsupportedOperationException(
                "It is not known how to convert a value of type "
                        + value.getClass().getSimpleName() + " to primitive long.");
    }

    static long updateInterval(int value, TimeUnit unit) {
        return unit.toMillis(value);
    }

    public static void probeIn(ProbingCycle cycle, File f) {
        cycle.probe("freeSpace", f.getFreeSpace());
        cycle.probe("totalSpace", f.getTotalSpace());
        cycle.probe("usableSpace", f.getUsableSpace());
        cycle.probe("creationTime", f.lastModified());
    }

    public static void probeIn(ProbingCycle cycle, ClassLoadingMXBean bean) {
        cycle.probe("loadedClassesCount", bean.getLoadedClassCount());
        cycle.probe("totalLoadedClassesCount", bean.getTotalLoadedClassCount());
        cycle.probe("unloadedClassCount", bean.getUnloadedClassCount());
    }

    public static void probeIn(ProbingCycle cycle, Runtime runtime) {
        long free = runtime.freeMemory();
        long total = runtime.totalMemory();
        cycle.probe("freeMemory", free);
        cycle.probe("totalMemory", total);
        cycle.probe("usedMemory", total - free);
        cycle.probe("maxMemory", runtime.maxMemory());
        cycle.probe("availableProcessors", runtime.availableProcessors());
    }

    public static void probeIn(ProbingCycle cycle, RuntimeMXBean bean) {
        cycle.probe("uptime", bean.getUptime());
    }

    public static void probeIn(ProbingCycle cycle, ThreadMXBean bean) {
        cycle.probe("threadCount", bean.getThreadCount());
        cycle.probe("peakThreadCount", bean.getPeakThreadCount());
        cycle.probe("daemonThreadCount", bean.getDaemonThreadCount());
        cycle.probe("totalStartedThreadCount", bean.getTotalStartedThreadCount());
    }

    public static void probeIn(ProbingCycle cycle, String type, Thread[] threads) {
        if (threads.length == 0) {
            return; // avoid unnecessary context manipulation
        }
        Tags tags = cycle.openContext().tag(TAG_TYPE, type);
        for (int i = 0; i < threads.length; i++) {
            tags.tag(TAG_INSTANCE, threads[i].getName());
            cycle.probe(threads[i]);
        }
    }

    public static <T> void probeAllInstances(ProbingCycle cycle, String type, Map<String, T> entries) {
        if (entries.isEmpty()) {
            return; // avoid unnecessary context manipulation
        }
        Tags tags = cycle.openContext().tag(TAG_TYPE, type);
        for (Entry<String, T> e : entries.entrySet()) {
            tags.tag(TAG_INSTANCE, e.getKey());
            cycle.probe(e.getValue());
        }
    }

    public static <T extends LocalDistributedObjectStats> void probeIn(ProbingCycle cycle,
            String type, Map<String, T> stats) {
        if (stats.isEmpty()) {
            return; // avoid unnecessary context manipulation
        }
        ProbingCycle.Tags tags = cycle.openContext().tag(TAG_TYPE, type);
        for (Entry<String, T> e : stats.entrySet()) {
            T val = e.getValue();
            if (val.isStatisticsEnabled()) {
                tags.tag(TAG_INSTANCE, e.getKey());
                cycle.probe(val);
                if (val instanceof LocalMapStatsImpl) {
                    LocalMapStatsImpl mapStats = (LocalMapStatsImpl) val;
                    NearCacheStats nearCacheStats = mapStats.getNearCacheStats();
                    if (nearCacheStats != null) {
                        cycle.probe("nearcache", nearCacheStats);
                    }
                    Map<String, LocalIndexStats> indexStats = mapStats.getIndexStats();
                    if (indexStats != null && !indexStats.isEmpty()) {
                        for (Entry<String, LocalIndexStats> index : indexStats.entrySet()) {
                            tags.tag("index", index.getKey());
                            cycle.probe(index.getValue());
                        }
                        // restore context after adding 2nd tag
                        tags = cycle.openContext().tag(TAG_TYPE, type); 
                    }
                }
            }
        }
    }

    public static void probeClientStats(ProbingCycle cycle, String uuid, CharSequence stats) {
        if (stats == null) {
            return;
        }
        if (startsWith("1\n", stats)) { // protocol version 1 (since 3.12)
            Lines lines = new Lines(stats);
            lines.next(); // gobble protocol
            cycle.openContext().tag("origin", uuid)
            .tag(TAG_TYPE, lines.next())
            .tag(TAG_INSTANCE, lines.next())
            .tag(TAG_TARGET, lines.next())
            .tag("version", lines.next());
            // this additional metric is used to convey client details via tags
            cycle.probe("principal", "?".contentEquals(lines.next()));
            cycle.openContext().tag("origin", uuid);
            lines.next();
            while (lines.length() > 0) {
                cycle.probeForwarded(lines.key(), lines.value());
                lines.next().next(); // first to end of current line as key goes back
            }
        }
    }

    private static final String[] PROBED_OS_METHODS = { "getCommittedVirtualMemorySize",
            "getFreePhysicalMemorySize", "getFreeSwapSpaceSize", "getProcessCpuTime",
            "getTotalPhysicalMemorySize", "getTotalSwapSpaceSize", "getMaxFileDescriptorCount",
            "getOpenFileDescriptorCount", "getProcessCpuLoad", "getSystemCpuLoad" };

    public static void probeIn(ProbingCycle cycle, OperatingSystemMXBean bean) {
        cycle.probe(MANDATORY, bean, PROBED_OS_METHODS);
        cycle.probe("systemLoadAverage", bean.getSystemLoadAverage());
    }


    /**
     * A {@link ProbeSource} providing information on runtime, threads,
     * class-loading and OS properties
     */
    public static final ProbeSource OS = new OsProbeSource();

    private static final class OsProbeSource implements ProbeSource {

        final Runtime runtime = Runtime.getRuntime();
        final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final File userHome = new File(System.getProperty("user.home"));
        final ClassLoadingMXBean classLoadingMXBean = ManagementFactory.getClassLoadingMXBean();
        final OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();

        @Override
        public void probeIn(ProbingCycle cycle) {
            cycle.openContext().prefix("runtime");
            Probing.probeIn(cycle, runtime);
            Probing.probeIn(cycle, runtimeMXBean);
            cycle.openContext().prefix("thread");
            Probing.probeIn(cycle, threadMXBean);
            cycle.openContext().prefix("classloading");
            Probing.probeIn(cycle, classLoadingMXBean);
            cycle.openContext().prefix("os");
            Probing.probeIn(cycle, operatingSystemMXBean);
            cycle.openContext().tag(TAG_TYPE, "file.partition").tag(TAG_INSTANCE, "user.home");
            Probing.probeIn(cycle, userHome);
        }

    }

    /**
     * A {@link ProbeSource} providing information about GC activity
     */
    public static final ProbeSource GC = new GcProbeSource();

    private static final class GcProbeSource implements ProbeSource {

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

}
