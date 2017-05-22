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

import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.ProbeFunction;
import com.hazelcast.internal.metrics.StringGauge;

class StringGaugeImpl
        extends AbstractGauge implements StringGauge {

    private static final String DEFAULT_VALUE = null;

    StringGaugeImpl(MetricsRegistryImpl metricsRegistry, String name) {
        super(metricsRegistry, name);
    }

    @Override
    public String read() {
        ProbeInstance probeInstance = getProbeInstance();

        ProbeFunction function = null;
        Object source = null;

        if (probeInstance != null) {
            function = probeInstance.function;
            source = probeInstance.source;
        }

        if (function == null || source == null) {
            clearProbeInstance();
            return DEFAULT_VALUE;
        }

        try {
            if (function instanceof LongProbeFunction) {
                LongProbeFunction longFunction = (LongProbeFunction) function;
                return String.valueOf(longFunction.get(source));
            } else {
                DoubleProbeFunction doubleFunction = (DoubleProbeFunction) function;
                double doubleResult = doubleFunction.get(source);
                return String.valueOf(Math.round(doubleResult));
            }
        } catch (Exception e) {
            metricsRegistry.logger.warning("Failed to access probe:" + name, e);
            return DEFAULT_VALUE;
        }
    }
}
