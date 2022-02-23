/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.Gauge;
import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.ProbeFunction;

/**
 * {@link Gauge} implementation that can be used to the last seen value
 * as long from a particular metric.
 * <p/>
 * This implementation distinguishes between static and dynamic metric
 * sources. The dynamic metrics are by design discovered during the
 * collection cycle, while for the static metrics there is always a
 * {@link ProbeInstance} that the gauge's value can be read from. Rendering
 * the most up-to-date value is a spoken goal of the gauges, hence we need
 * this distinction between the logic that reads out the value of the gauge
 * from the metrics' sources. This distinction is done by introducing the
 * internal {@link LongGaugeSource} interface and two internal
 * implementations:
 * <ul>
 * <li>{@link ProbeInstanceGaugeSource} for static metrics and</li>
 * <li>{@link LongMetricValueCatcher} for dynamic metrics</li>
 * </ul>
 */
class LongGaugeImpl extends AbstractGauge implements LongGauge {

    static final long DEFAULT_VALUE = 0L;

    /**
     * The value of this gauge is read out from this gauge source. Needed
     * to handle static and dynamic metrics transparently for the gauge
     * internals.
     * <p/>
     * If this gauge is created for a static metric, this is a
     * {@link ProbeInstanceGaugeSource} instance, otherwise a
     * {@link LongMetricValueCatcher} instance. The latter is used to
     * catch the value of the dynamic metric this gauge is created for.
     * If a static metric with metric id matching this gauge's name is
     * created after creating this gauge, the
     * {@link LongMetricValueCatcher} instance is replaced with a
     * {@link ProbeInstanceGaugeSource} instance.
     */
    private volatile LongGaugeSource gaugeSource;

    LongGaugeImpl(MetricsRegistryImpl metricsRegistry, String name) {
        super(metricsRegistry, name);
        ProbeInstance probeInstance = metricsRegistry.getProbeInstance(name);
        if (probeInstance != null) {
            this.gaugeSource = new ProbeInstanceGaugeSource(probeInstance);
        } else {
            this.gaugeSource = new LongMetricValueCatcher();
        }
    }

    @Override
    public long read() {
        return gaugeSource.read();
    }

    @Override
    LongMetricValueCatcher getCatcherOrNull() {
        // we take a defensive copy, since this.gaugeSource might be changed
        // between the instanceof and the casting
        LongGaugeSource gaugeSourceCopy = this.gaugeSource;
        return gaugeSourceCopy instanceof LongMetricValueCatcher ? (LongMetricValueCatcher) gaugeSourceCopy : null;
    }

    @Override
    public void render(StringBuilder stringBuilder) {
        stringBuilder.append(read());
    }

    @Override
    public void onProbeInstanceSet(ProbeInstance probeInstance) {
        gaugeSource = new ProbeInstanceGaugeSource(probeInstance);
    }

    private static long getMetricValue(String gaugeName, Object source, ProbeFunction function,
                                       MetricsRegistryImpl metricsRegistry) {
        try {
            if (function instanceof LongProbeFunction) {
                LongProbeFunction longFunction = (LongProbeFunction) function;
                return longFunction.get(source);
            } else {
                DoubleProbeFunction doubleFunction = (DoubleProbeFunction) function;
                double doubleResult = doubleFunction.get(source);
                return Math.round(doubleResult);
            }
        } catch (Exception e) {
            metricsRegistry.logger.warning("Failed to access the probe: " + gaugeName, e);
            return DEFAULT_VALUE;
        }
    }

    private interface LongGaugeSource {
        long read();
    }

    private final class LongMetricValueCatcher extends AbstractMetricValueCatcher implements LongGaugeSource {
        private volatile long value = DEFAULT_VALUE;

        @Override
        public void catchMetricValue(long collectionId, long value) {
            lastCollectionId = collectionId;
            this.value = value;
            clearCachedMetricSourceRef();
        }

        @Override
        public void catchMetricValue(long collectionId, double value) {
            lastCollectionId = collectionId;
            this.value = Math.round(value);
            clearCachedMetricSourceRef();
        }

        @Override
        public long read() {
            return readBase(LongGaugeImpl::getMetricValue, () -> value);
        }

        @Override
        void clearCachedValue() {
            value = DEFAULT_VALUE;
        }
    }

    private final class ProbeInstanceGaugeSource implements LongGaugeSource {

        private volatile ProbeInstance probeInstance;

        private ProbeInstanceGaugeSource(ProbeInstance probeInstance) {
            this.probeInstance = probeInstance;
        }

        @Override
        public long read() {
            ProbeFunction function = null;
            Object source = null;

            if (probeInstance != null) {
                function = probeInstance.function;
                source = probeInstance.source;
            }

            if (function == null || source == null) {
                probeInstance = null;
                return DEFAULT_VALUE;
            }

            return getMetricValue(name, source, function, metricsRegistry);
        }
    }
}
