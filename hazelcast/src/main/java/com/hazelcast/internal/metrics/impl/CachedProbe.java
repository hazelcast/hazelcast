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

import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;

/**
 * Class for caching the fields of a {@link Probe} to avoid allocation
 * through reflective access.
 */
final class CachedProbe {
    private final ProbeLevel level;
    private final ProbeUnit unit;
    private final MetricTarget[] excludedTargets;

    CachedProbe(Probe probe) {
        unit = probe.unit();
        level = probe.level();
        excludedTargets = probe.excludedTargets();
    }

    ProbeLevel level() {
        return level;
    }

    ProbeUnit unit() {
        return unit;
    }

    MetricTarget[] excludedTargets() {
        return excludedTargets;
    }
}
