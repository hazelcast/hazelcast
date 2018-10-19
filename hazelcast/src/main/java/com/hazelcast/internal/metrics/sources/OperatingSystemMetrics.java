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

package com.hazelcast.internal.metrics.sources;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import com.hazelcast.internal.metrics.BeforeCollectionCycle;
import com.hazelcast.internal.metrics.CollectionCycle;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import com.hazelcast.internal.metrics.MetricsSource;
import com.hazelcast.internal.metrics.Namespace;
import com.hazelcast.internal.metrics.Probe;

@SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "used for metrics via reflection")
@Namespace("os")
public final class OperatingSystemMetrics implements MetricsSource {

    private static final String[] PROBED_OS_METHODS = { "getCommittedVirtualMemorySize",
            "getFreePhysicalMemorySize", "getFreeSwapSpaceSize", "getProcessCpuTime",
            "getTotalPhysicalMemorySize", "getTotalSwapSpaceSize", "getMaxFileDescriptorCount",
            "getOpenFileDescriptorCount", "getProcessCpuLoad", "getSystemCpuLoad" };

    private final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

    @Probe(level = MANDATORY)
    private double systemLoadAverage;

    @BeforeCollectionCycle
    private void update() {
        systemLoadAverage = os.getSystemLoadAverage();
    }

    @Override
    public void collectAll(CollectionCycle cycle) {
        cycle.switchContext().namespace("os");
        cycle.collect(MANDATORY, os, PROBED_OS_METHODS);
    }
}
