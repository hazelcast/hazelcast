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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStore;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.internal.util.ConcurrentReferenceHashMap;
import com.hazelcast.internal.util.ConcurrentReferenceHashMap.ReferenceType;
import com.hazelcast.internal.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * A {@link DiagnosticsPlugin} that helps to detect if there are any performance issues with Stores/Loaders like e.g.
 * {@link MapStore}.
 * <p>
 * This is done by instrumenting these Stores/Loaders with latency tracking probes, so that per Store/Loader all kinds
 * of statistics like count, avg, mag, latency distribution etc is available.
 * <p>
 * One of the main purposes of this plugin is to make sure that e.g. a Database is the cause of slow performance. This
 * plugin is useful to be combined with {@link SlowOperationPlugin} to get idea about where the threads are spending
 * their time.
 * <p>
 * If this plugin is not enabled, there is no performance hit since the Stores/Loaders don't get decorated.
 */
public class StoreLatencyPlugin extends DiagnosticsPlugin {

    /**
     * The period in seconds this plugin runs.
     * <p>
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty PERIOD_SECONDS
            = new HazelcastProperty("hazelcast.diagnostics.storeLatency.period.seconds", 0, SECONDS);

    /**
     * The period in second the statistics should be reset. Normally the statistics are not reset and if the system
     * is running for an extended time, it isn't possible to see that happened in e.g. the last hour.
     * <p>
     * Currently there is no sliding window functionality to deal with this correctly. But for the time being this
     * setting will periodically reset the statistics.
     */
    public static final HazelcastProperty RESET_PERIOD_SECONDS
            = new HazelcastProperty("hazelcast.diagnostics.storeLatency.reset.period.seconds", 0, SECONDS);

    private static final int LOW_WATERMARK_MICROS = 100;

    private static final int LATENCY_BUCKET_COUNT = 32;

    private static final String[] LATENCY_KEYS;

    static {
        LATENCY_KEYS = new String[LATENCY_BUCKET_COUNT];
        long maxDurationForBucket = LOW_WATERMARK_MICROS;
        long p = 0;
        for (int k = 0; k < LATENCY_KEYS.length; k++) {
            LATENCY_KEYS[k] = p + ".." + (maxDurationForBucket - 1) + "us";
            p = maxDurationForBucket;
            maxDurationForBucket *= 2;
        }
    }

    private final ConcurrentMap<String, ServiceProbes> metricsPerServiceMap
            = new ConcurrentHashMap<String, ServiceProbes>();

    private final ConstructorFunction<String, ServiceProbes> metricsPerServiceConstructorFunction
            = ServiceProbes::new;

    private final ConstructorFunction<String, InstanceProbes> instanceProbesConstructorFunction
            = InstanceProbes::new;

    private final long periodMillis;
    private final long resetPeriodMillis;
    private final long resetFrequency;

    private long iteration;

    public StoreLatencyPlugin(NodeEngineImpl nodeEngine) {
        this(nodeEngine.getLogger(StoreLatencyPlugin.class), nodeEngine.getProperties());
    }

    public StoreLatencyPlugin(ILogger logger, HazelcastProperties properties) {
        super(logger);
        this.periodMillis = properties.getMillis(PERIOD_SECONDS);
        this.resetPeriodMillis = properties.getMillis(RESET_PERIOD_SECONDS);
        if (periodMillis == 0 || resetPeriodMillis == 0) {
            this.resetFrequency = 0;
        } else {
            this.resetFrequency = max(1, resetPeriodMillis / periodMillis);
        }
    }

    @Override
    public long getPeriodMillis() {
        return periodMillis;
    }

    @Override
    public void onStart() {
        logger.info("Plugin:active: period-millis:" + periodMillis + " resetPeriod-millis:" + resetPeriodMillis);
    }

    @Override
    public void run(DiagnosticsLogWriter writer) {
        iteration++;
        render(writer);
        resetStatisticsIfNeeded();
    }

    private void render(DiagnosticsLogWriter writer) {
        for (ServiceProbes serviceProbes : metricsPerServiceMap.values()) {
            serviceProbes.render(writer);
        }
    }

    private void resetStatisticsIfNeeded() {
        if (resetFrequency > 0 && iteration % resetFrequency == 0) {
            for (ServiceProbes serviceProbes : metricsPerServiceMap.values()) {
                serviceProbes.resetStatistics();
            }
        }
    }

    // just for testing
    public long count(String serviceName, String dataStructureName, String methodName) {
        return ((LatencyProbeImpl) newProbe(serviceName, dataStructureName, methodName)).stats.count;
    }

    public LatencyProbe newProbe(String serviceName, String dataStructureName, String methodName) {
        ServiceProbes serviceProbes = getOrPutIfAbsent(
                metricsPerServiceMap, serviceName, metricsPerServiceConstructorFunction);
        return serviceProbes.newProbe(dataStructureName, methodName);
    }

    private final class ServiceProbes {

        private final String serviceName;

        // the InstanceProbes are stored in a weak reference, so that if the store/loader is gc'ed, the probes are gc'ed
        // and therefor there is no strong reference to the InstanceProbes and they get gc'ed
        private final ConcurrentReferenceHashMap<String, InstanceProbes> instanceProbesMap
                = new ConcurrentReferenceHashMap<String, InstanceProbes>(ReferenceType.STRONG, ReferenceType.WEAK);

        private ServiceProbes(String serviceName) {
            this.serviceName = serviceName;
        }

        private LatencyProbe newProbe(String dataStructureName, String methodName) {
            InstanceProbes instanceProbes = getOrPutIfAbsent(
                    instanceProbesMap, dataStructureName, instanceProbesConstructorFunction);
            return instanceProbes.newProbe(methodName);
        }

        private void render(DiagnosticsLogWriter writer) {
            writer.startSection(serviceName);

            for (InstanceProbes instanceProbes : instanceProbesMap.values()) {
                instanceProbes.render(writer);
            }
            writer.endSection();
        }

        private void resetStatistics() {
            for (InstanceProbes instanceProbes : instanceProbesMap.values()) {
                instanceProbes.resetStatistics();
            }
        }
    }

    /**
     * Contains all probes for a given instance, e.g. for an {@link IMap} instance {@code employees}.
     */
    private static final class InstanceProbes {

        // key is the name of the probe, e.g. load
        private final ConcurrentMap<String, LatencyProbeImpl> probes = new ConcurrentHashMap<String, LatencyProbeImpl>();
        private final String dataStructureName;

        InstanceProbes(String dataStructureName) {
            this.dataStructureName = dataStructureName;
        }

        LatencyProbe newProbe(String methodName) {
            LatencyProbeImpl probe = probes.get(methodName);
            if (probe == null) {
                LatencyProbeImpl newProbe = new LatencyProbeImpl(methodName, this);
                LatencyProbeImpl found = probes.putIfAbsent(methodName, newProbe);
                probe = found == null ? newProbe : found;
            }
            return probe;
        }

        private void render(DiagnosticsLogWriter writer) {
            writer.startSection(dataStructureName);
            for (LatencyProbeImpl probe : probes.values()) {
                probe.render(writer);
            }
            writer.endSection();
        }

        private void resetStatistics() {
            for (LatencyProbeImpl probe : probes.values()) {
                probe.resetStatistics();
            }
        }
    }

    // package private for testing
    static final class LatencyProbeImpl implements LatencyProbe {

        // instead of storing it in a final field, it is stored in a volatile field because stats can be reset
        volatile Statistics stats = new Statistics();

        // a strong reference to prevent garbage collection
        @SuppressWarnings("unused")
        private final InstanceProbes instanceProbes;

        private final String methodName;

        private LatencyProbeImpl(String methodName, InstanceProbes instanceProbes) {
            this.methodName = methodName;
            this.instanceProbes = instanceProbes;
        }

        @Override
        public void recordValue(long durationNanos) {
            stats.recordValue(durationNanos);
        }

        private void render(DiagnosticsLogWriter writer) {
            Statistics stats = this.stats;
            long invocations = stats.count;
            long totalMicros = stats.totalMicros;
            long avgMicros = invocations == 0 ? 0 : totalMicros / invocations;
            long maxMicros = stats.maxMicros;

            if (invocations == 0) {
                return;
            }

            writer.startSection(methodName);
            writer.writeKeyValueEntry("count", invocations);
            writer.writeKeyValueEntry("totalTime(us)", totalMicros);
            writer.writeKeyValueEntry("avg(us)", avgMicros);
            writer.writeKeyValueEntry("max(us)", maxMicros);

            writer.startSection("latency-distribution");
            for (int k = 0; k < stats.latencyDistribution.length(); k++) {
                long value = stats.latencyDistribution.get(k);
                if (value > 0) {
                    writer.writeKeyValueEntry(LATENCY_KEYS[k], value);
                }
            }
            writer.endSection();

            writer.endSection();
        }

        private void resetStatistics() {
            stats = new Statistics();
        }
    }

    static final class Statistics {

        private static final AtomicLongFieldUpdater<Statistics> COUNT
                = newUpdater(Statistics.class, "count");
        private static final AtomicLongFieldUpdater<Statistics> TOTAL_MICROS
                = newUpdater(Statistics.class, "totalMicros");
        private static final AtomicLongFieldUpdater<Statistics> MAX_MICROS
                = newUpdater(Statistics.class, "maxMicros");

        volatile long count;
        volatile long maxMicros;
        volatile long totalMicros;

        private final AtomicLongArray latencyDistribution = new AtomicLongArray(LATENCY_BUCKET_COUNT);

        private void recordValue(long durationNanos) {
            long durationMicros = NANOSECONDS.toMicros(durationNanos);

            COUNT.addAndGet(this, 1);
            TOTAL_MICROS.addAndGet(this, durationMicros);

            for (; ; ) {
                long currentMax = maxMicros;
                if (durationMicros <= currentMax) {
                    break;
                }

                if (MAX_MICROS.compareAndSet(this, currentMax, durationMicros)) {
                    break;
                }
            }

            int bucketIndex = 0;
            long maxDurationForBucket = LOW_WATERMARK_MICROS;
            for (int k = 0; k < latencyDistribution.length() - 1; k++) {
                if (durationMicros >= maxDurationForBucket) {
                    bucketIndex++;
                    maxDurationForBucket *= 2;
                } else {
                    break;
                }
            }

            latencyDistribution.incrementAndGet(bucketIndex);
        }
    }

    public interface LatencyProbe {
        void recordValue(long latencyNanos);
    }
}
