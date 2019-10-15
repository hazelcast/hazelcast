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

package com.hazelcast.internal.metrics.collectors;

import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.MetricsRegistry;

import java.util.Set;

/**
 * With the {@link MetricsCollector} the metrics registered in the
 * {@link MetricsRegistry} can be collected.
 */
public interface MetricsCollector {

    void collectLong(String name, long value, Set<MetricTarget> excludedTargets);

    void collectDouble(String name, double value, Set<MetricTarget> excludedTargets);

    void collectException(String name, Exception e, Set<MetricTarget> excludedTargets);

    void collectNoValue(String name, Set<MetricTarget> excludedTargets);

}
