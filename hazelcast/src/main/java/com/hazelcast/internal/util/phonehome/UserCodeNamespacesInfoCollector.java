/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.UserCodeNamespacesConfig;
import com.hazelcast.instance.impl.Node;

import java.util.function.BiConsumer;

public class UserCodeNamespacesInfoCollector implements MetricsCollector {

    @Override
    public void forEachMetric(Node node, BiConsumer<PhoneHomeMetrics, String> metricsConsumer) {
        UserCodeNamespacesConfig nsConfigs = node.getConfig().getNamespacesConfig();
        boolean enabled = nsConfigs.isEnabled();
        metricsConsumer.accept(PhoneHomeMetrics.UCN_ENABLED, String.valueOf(enabled));
        if (enabled) {
            metricsConsumer.accept(PhoneHomeMetrics.UCN_NAMESPACE_COUNT,
                    String.valueOf(ConfigAccessor.getNamespaceConfigs(nsConfigs).size()));
        }
    }
}
