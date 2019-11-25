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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.ProbeLevel;

import java.util.Collection;
import java.util.Set;

import static com.hazelcast.internal.metrics.MetricTarget.ALL_TARGETS;
import static com.hazelcast.internal.metrics.MetricTarget.ALL_TARGETS_BUT_DIAGNOSTICS;
import static com.hazelcast.internal.metrics.MetricTarget.DIAGNOSTICS;
import static com.hazelcast.internal.metrics.MetricTarget.asSet;
import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static java.util.Collections.emptySet;

final class MetricsUtil {
    private MetricsUtil() {
    }

    static Collection<MetricTarget> excludedTargets(Collection<MetricTarget> exclusions, ProbeLevel level) {
        if (DEBUG != level) {
            return exclusions;
        } else if (exclusions.contains(DIAGNOSTICS)) {
            return ALL_TARGETS;
        } else {
            return ALL_TARGETS_BUT_DIAGNOSTICS;
        }
    }

    static Collection<MetricTarget> getExcludedTargets(Object object) {
        if (object instanceof FieldProbe) {
            FieldProbe fieldProbe = (FieldProbe) object;
            return getMetricTargets(fieldProbe.probe, fieldProbe.sourceMetadata);
        }

        if (object instanceof MethodProbe) {
            MethodProbe methodProbe = (MethodProbe) object;
            return getMetricTargets(methodProbe.probe, methodProbe.sourceMetadata);
        }

        return emptySet();
    }

    private static Collection<MetricTarget> getMetricTargets(CachedProbe probe, SourceMetadata sourceMetadata) {
        ProbeLevel level = probe.level();
        Collection<MetricTarget> excludedTargetsClass = sourceMetadata.excludedTargetsClass();
        Set<MetricTarget> excludedTargetsProbe = asSet(probe.excludedTargets());
        Set<MetricTarget> excludedTargetsUnion = MetricTarget.union(excludedTargetsClass, excludedTargetsProbe);

        return excludedTargets(excludedTargetsUnion, level);
    }

}
