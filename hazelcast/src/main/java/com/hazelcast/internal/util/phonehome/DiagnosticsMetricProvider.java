/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.phonehome;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.diagnostics.Diagnostics;

import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.DIAGNOSTICS_DYNAMIC_AUTO_OFF_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.DIAGNOSTICS_DYNAMIC_ENABLED_COUNT;

public class DiagnosticsMetricProvider implements MetricsProvider {
    @Override
    public void provideMetrics(Node node, MetricsCollectionContext context) {

        Diagnostics diagnostics = node.getNodeEngine().getDiagnostics();

        context.collect(DIAGNOSTICS_DYNAMIC_ENABLED_COUNT,
                diagnostics.getMetricCollector().getDynamicallyEnabledCount().get());
        context.collect(DIAGNOSTICS_DYNAMIC_AUTO_OFF_COUNT,
                diagnostics.getMetricCollector().getAutoOffDisabledCount().get());
    }
}
