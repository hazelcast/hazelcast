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

package com.hazelcast.jet.impl.metrics;

import com.hazelcast.jet.core.metrics.Metric;
import com.hazelcast.jet.core.metrics.Unit;
import com.hazelcast.jet.impl.execution.init.Contexts;

public final class MetricsImpl {

    private MetricsImpl() {
    }

    public static Metric metric(String name, Unit unit) {
        return getMetricsContext().metric(name, unit);
    }

    public static Metric threadSafeMetric(String name, Unit unit) {
        return getMetricsContext().threadSafeMetric(name, unit);
    }

    private static MetricsContext getMetricsContext() {
        return Contexts.getCastedThreadContext().metricsContext();
    }
}
