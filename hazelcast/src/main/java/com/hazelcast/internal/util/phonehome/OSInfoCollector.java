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

package com.hazelcast.internal.util.phonehome;

import com.hazelcast.instance.impl.Node;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.function.BiConsumer;

class OSInfoCollector implements MetricsCollector {

    @Override
    public void forEachMetric(Node node, BiConsumer<PhoneHomeMetrics, String> metricsConsumer) {
        OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
        try {
            metricsConsumer.accept(PhoneHomeMetrics.OPERATING_SYSTEM_NAME, osMxBean.getName());
            metricsConsumer.accept(PhoneHomeMetrics.OPERATING_SYSTEM_ARCH, osMxBean.getArch());
            metricsConsumer.accept(PhoneHomeMetrics.OPERATING_SYSTEM_VERSION, osMxBean.getVersion());
        } catch (SecurityException e) {
            metricsConsumer.accept(PhoneHomeMetrics.OPERATING_SYSTEM_NAME, "N/A");
            metricsConsumer.accept(PhoneHomeMetrics.OPERATING_SYSTEM_ARCH, "N/A");
            metricsConsumer.accept(PhoneHomeMetrics.OPERATING_SYSTEM_VERSION, "N/A");
        }
    }
}
