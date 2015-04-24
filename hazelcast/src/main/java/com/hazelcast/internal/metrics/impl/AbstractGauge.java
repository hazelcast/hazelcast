/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.Metric;

abstract class AbstractGauge implements Metric {

    protected final MetricsRegistryImpl metricsRegistry;
    protected final String name;
    private volatile ProbeInstance probeInstance;

    AbstractGauge(MetricsRegistryImpl metricsRegistry, String name) {
        this.metricsRegistry = metricsRegistry;
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    void clearProbeInstance() {
        probeInstance = null;
    }

    ProbeInstance getProbeInstance() {
        ProbeInstance probeInstance = this.probeInstance;
        if (probeInstance == null) {
            probeInstance = metricsRegistry.getProbeInstance(name);
            this.probeInstance = probeInstance;
        }
        return probeInstance;
    }
}
