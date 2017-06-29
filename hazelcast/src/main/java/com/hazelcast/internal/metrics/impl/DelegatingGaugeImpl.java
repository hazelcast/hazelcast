/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.Gauge;

import static com.hazelcast.internal.metrics.impl.MetricsRegistryImpl.isUnavailableProbe;
import static com.hazelcast.internal.metrics.impl.MetricsRegistryImpl.toGaugeFromProbe;

/**
 * Lazily initializes matching gauge or renders NA when there is no probe.
 */
class DelegatingGaugeImpl extends AbstractGauge implements Gauge {

    static final String NOT_AVAILABLE = "NA";

    volatile Gauge gauge;

    DelegatingGaugeImpl(MetricsRegistryImpl metricsRegistry, String name) {
        super(metricsRegistry, name);
    }

    @Override
    public void render(StringBuilder stringBuilder) {
        if (gauge != null) {
            gauge.render(stringBuilder);
            return;
        }

        ProbeInstance probeInstance = getProbeInstance();
        if (isUnavailableProbe(probeInstance)) {
            stringBuilder.append(NOT_AVAILABLE);
            return;
        }

        gauge = toGaugeFromProbe(probeInstance, metricsRegistry, name, false);
    }
}
