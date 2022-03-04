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

import com.hazelcast.internal.metrics.DoubleGauge;
import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.ProbeFunction;

class DoubleGaugeImpl extends AbstractGauge implements DoubleGauge {

    static final double DEFAULT_VALUE = 0d;

    /**
     * The value of this gauge is read out from this gauge source. Needed
     * to handle static and dynamic metrics transparently for the gauge
     * internals.
     * <p/>
     * If this gauge is created for a static metric, this is a
     * {@link ProbeInstanceGaugeSource} instance, otherwise a
     * {@link DoubleMetricValueCatcher} instance. The latter is used to
     * catch the value of the dynamic metric this gauge is created for.
     * If a static metric with metric id matching this gauge's name is
     * created after creating this gauge, the
     * {@link DoubleMetricValueCatcher} instance is replaced with a
     * {@link ProbeInstanceGaugeSource} instance.
     */
    private volatile DoubleGaugeSource gaugeSource;

    DoubleGaugeImpl(MetricsRegistryImpl metricsRegistry, String name) {
        super(metricsRegistry, name);
        ProbeInstance probeInstance = metricsRegistry.getProbeInstance(name);
        if (probeInstance != null) {
            this.gaugeSource = new ProbeInstanceGaugeSource(probeInstance);
        } else {
            this.gaugeSource = new DoubleMetricValueCatcher();
        }
    }

    @Override
    public void onProbeInstanceSet(ProbeInstance probeInstance) {
        gaugeSource = new ProbeInstanceGaugeSource(probeInstance);
    }

    @Override
    MetricValueCatcher getCatcherOrNull() {
        // we take a defensive copy, since this.gaugeSource might be changed
        // between the instanceof and the casting
        DoubleGaugeSource gaugeSourceCopy = this.gaugeSource;
        return gaugeSourceCopy instanceof DoubleMetricValueCatcher ? (DoubleMetricValueCatcher) gaugeSourceCopy : null;
    }

    @Override
    public double read() {
        return gaugeSource.read();
    }

    @Override
    public void render(StringBuilder stringBuilder) {
        stringBuilder.append(read());
    }

    private static double getMetricValue(String gaugeName, Object source, ProbeFunction function,
                                         MetricsRegistryImpl metricsRegistry) {
        try {
            if (function instanceof LongProbeFunction) {
                LongProbeFunction longFunction = (LongProbeFunction) function;
                return longFunction.get(source);
            } else {
                DoubleProbeFunction doubleFunction = (DoubleProbeFunction) function;
                return doubleFunction.get(source);
            }
        } catch (Exception e) {
            metricsRegistry.logger.warning("Failed to access the probe: " + gaugeName, e);
            return DEFAULT_VALUE;
        }
    }

    /**
     * Local interface used to transparently back this gauge with static
     * or dynamic metrics.
     *
     * @see #gaugeSource
     */
    private interface DoubleGaugeSource {
        double read();
    }

    private final class DoubleMetricValueCatcher extends AbstractMetricValueCatcher implements DoubleGaugeSource {
        private volatile double value = DEFAULT_VALUE;

        @Override
        public void catchMetricValue(long collectionId, long value) {
            lastCollectionId = collectionId;
            this.value = value;
            clearCachedMetricSourceRef();
        }

        @Override
        public void catchMetricValue(long collectionId, double value) {
            lastCollectionId = collectionId;
            this.value = value;
            clearCachedMetricSourceRef();
        }

        @Override
        public double read() {
            return readBase(DoubleGaugeImpl::getMetricValue, () -> value);
        }

        @Override
        void clearCachedValue() {
            value = DEFAULT_VALUE;
        }
    }

    private final class ProbeInstanceGaugeSource implements DoubleGaugeSource {

        private volatile ProbeInstance probeInstance;

        private ProbeInstanceGaugeSource(ProbeInstance probeInstance) {
            this.probeInstance = probeInstance;
        }

        @Override
        public double read() {
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
