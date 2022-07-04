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

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.ProbeFunction;
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

    /**
     * Adjusts the excluded targets on the given {@code descriptor} if
     * the provided {@code level} is {@link ProbeLevel#DEBUG}.
     *
     * @param descriptor The descriptor to update
     * @param level      The level of the metric to adjust with
     * @param minimumLevel      The level of metrics exposed for debugging purposes
     */
    static void adjustExclusionsWithLevel(MetricDescriptor descriptor, ProbeLevel level, ProbeLevel minimumLevel) {
        if (DEBUG == minimumLevel) {
            return;
        }

        if (DEBUG != level) {
            return;
        }

        if (descriptor.excludedTargets().contains(DIAGNOSTICS)) {
            descriptor.withExcludedTargets(ALL_TARGETS);
        } else {
            descriptor.withExcludedTargets(ALL_TARGETS_BUT_DIAGNOSTICS);
        }
    }

    /**
     * Extract the excluded targets from the given {@link ProbeFunction}.
     *
     * @param function The {@link ProbeFunction} the excluded targets to
     *                 be excluded from
     * @param minimumLevel The level of metrics exposed for debugging purposes
     * @return the excluded targets
     */
    static Collection<MetricTarget> extractExcludedTargets(ProbeFunction function, ProbeLevel minimumLevel) {
        if (function instanceof FieldProbe) {
            FieldProbe fieldProbe = (FieldProbe) function;
            return extractExcludedTargets(fieldProbe.probe, fieldProbe.sourceMetadata, minimumLevel);
        }

        if (function instanceof MethodProbe) {
            MethodProbe methodProbe = (MethodProbe) function;
            return extractExcludedTargets(methodProbe.probe, methodProbe.sourceMetadata, minimumLevel);
        }

        return emptySet();
    }

    private static Collection<MetricTarget> extractExcludedTargets(CachedProbe probe, SourceMetadata sourceMetadata,
                                                                   ProbeLevel minimumLevel) {
        ProbeLevel level = probe.level();
        Collection<MetricTarget> excludedTargetsClass = sourceMetadata.excludedTargetsClass();
        Set<MetricTarget> excludedTargetsProbe = asSet(probe.excludedTargets());
        Set<MetricTarget> excludedTargetsUnion = MetricTarget.union(excludedTargetsClass, excludedTargetsProbe);

        if (DEBUG == minimumLevel) {
            return excludedTargetsUnion;
        }

        if (DEBUG != level) {
            return excludedTargetsUnion;
        } else if (excludedTargetsUnion.contains(DIAGNOSTICS)) {
            return ALL_TARGETS;
        } else {
            return ALL_TARGETS_BUT_DIAGNOSTICS;
        }
    }
}
