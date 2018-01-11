/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics;

/**
 * A {@link MetricsProvider} that has the ability to discard to provided metrics. This is useful for dynamic metrics; so
 * metrics that get added and removed during the lifecycle of the MetricsRegistry like a connection.
 */
public interface DiscardableMetricsProvider extends MetricsProvider {

    void discardMetrics(MetricsRegistry registry);
}
