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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.util.ConcurrentReferenceHashMap;
import com.hazelcast.util.ConcurrentReferenceHashMap.ReferenceType;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.diagnostics.Diagnostics.PREFIX;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * A {@link DiagnosticsPlugin} that helps to detect if there are any performance issues with Stores/Loaders like e.g.
 * {@link com.hazelcast.core.MapStore}. This is done by instrumenting these Stores/Loaders with latency tracking probes,
 * so that per Store/Loader all kinds of statistics like count, avg, mag, latency distribution etc is available.
 *
 * One of the main purposes of this plugin is to make sure that e.g. a Database is the cause of slow performance. This
 * plugin is useful to be combined with {@link SlowOperationPlugin} to get idea about where the threads are spending
 * their time.
 *
 * If this plugin is not enabled, there is no performance hit since the Stores/Loaders don't get decorated.
 */
public class StoreLatencyPlugin extends DiagnosticsPlugin {

    /**
     * The period in seconds this plugin runs.
     *
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty PERIOD_SECONDS
            = new HazelcastProperty(PREFIX + ".storeLatency.period.seconds", 0, SECONDS);

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

    private final long periodMillis;

    private final ConcurrentMap<String, ServiceProbes> metricsPerServiceMap
            = new ConcurrentHashMap<String, ServiceProbes>();

    private final ConstructorFunction<String, ServiceProbes> metricsPerServiceConstructorFunction
            = new ConstructorFunction<String, ServiceProbes>() {
        @Override
        public ServiceProbes createNew(String serviceName) {
            return new ServiceProbes(serviceName);
        }
    };

    private final ConstructorFunction<String, InstanceProbes> instanceProbesConstructorFunction
            = new ConstructorFunction<String, InstanceProbes>() {
        @Override
        public InstanceProbes createNew(String dataStructureName) {
            return new InstanceProbes(dataStructureName);
        }
    };

    public StoreLatencyPlugin(NodeEngineImpl nodeEngine) {
        this(nodeEngine.getLogger(StoreLatencyPlugin.class), nodeEngine.getProperties());
    }

    public StoreLatencyPlugin(ILogger logger, HazelcastProperties properties) {
        super(logger);
        this.periodMillis = properties.getMillis(PERIOD_SECONDS);
    }

    @Override
    public long getPeriodMillis() {
        return periodMillis;
    }

    @Override
    public void onStart() {
        logger.info("Plugin:active: period-millis:" + periodMillis);
    }

    @Override
    public void run(DiagnosticsLogWriter writer) {
        for (ServiceProbes serviceProbes : metricsPerServiceMap.values()) {
            serviceProbes.render(writer);
        }
    }

    // just for testing
    public long count(String serviceName, String dataStructureName, String methodName) {
        return ((LatencyProbeImpl) newProbe(serviceName, dataStructureName, methodName)).count;
    }

    public LatencyProbe newProbe(String serviceName, String dataStructureName, String methodName) {
        ServiceProbes serviceProbes = getOrPutIfAbsent(
                metricsPerServiceMap, serviceName, metricsPerServiceConstructorFunction);
        return serviceProbes.newProbe(dataStructureName, methodName);
    }

    private final class ServiceProbes {

        private final String serviceName;

        // the InstanceProbes are stored in a weak reference. So that if the store/loader is gc'ed, the probes are gc'ed
        // and therefor there is no strong reference to the InstanceProbes and they get gc'ed.
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
    }

    /**
     * Contains all probes for a given instance, e.g. for an {@link com.hazelcast.core.IMap} instance {@code employees}.
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
    }

    // package private for testing
    static final class LatencyProbeImpl implements LatencyProbe {

        private static final AtomicLongFieldUpdater<LatencyProbeImpl> COUNT
                = newUpdater(LatencyProbeImpl.class, "count");
        private static final AtomicLongFieldUpdater<LatencyProbeImpl> TOTAL_MICROS
                = newUpdater(LatencyProbeImpl.class, "totalMicros");
        private static final AtomicLongFieldUpdater<LatencyProbeImpl> MAX_MICROS
                = newUpdater(LatencyProbeImpl.class, "maxMicros");

        volatile long count;
        volatile long maxMicros;
        volatile long totalMicros;

        private final AtomicLongArray latencyDistribution = new AtomicLongArray(LATENCY_BUCKET_COUNT);

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

        private void render(DiagnosticsLogWriter writer) {
            long invocations = this.count;
            long totalMicros = this.totalMicros;
            long avgMicros = invocations == 0 ? 0 : totalMicros / invocations;
            long maxMicros = this.maxMicros;

            if (invocations == 0) {
                return;
            }

            writer.startSection(methodName);
            writer.writeKeyValueEntry("count", invocations);
            writer.writeKeyValueEntry("totalTime(us)", totalMicros);
            writer.writeKeyValueEntry("avg(us)", avgMicros);
            writer.writeKeyValueEntry("max(us)", maxMicros);

            writer.startSection("latency-distribution");
            for (int k = 0; k < latencyDistribution.length(); k++) {
                long value = latencyDistribution.get(k);
                if (value > 0) {
                    writer.writeKeyValueEntry(LATENCY_KEYS[k], value);
                }
            }
            writer.endSection();

            writer.endSection();
        }
    }

    public interface LatencyProbe {
        void recordValue(long latencyNanos);
    }
}
