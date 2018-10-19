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
import java.lang.management.RuntimeMXBean;

import com.hazelcast.internal.metrics.BeforeCollectionCycle;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import com.hazelcast.internal.metrics.Namespace;
import com.hazelcast.internal.metrics.Probe;

@SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "used for metrics via reflection")
@Namespace("runtime")
public final class RuntimeMetrics {

    private final Runtime runtime = Runtime.getRuntime();
    private final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();

    @Probe(level = MANDATORY)
    private long availableProcessors;
    @Probe(level = MANDATORY)
    private long freeMemory;
    @Probe(level = MANDATORY)
    private long maxMemory;
    @Probe(level = MANDATORY)
    private long totalMemory;
    @Probe(level = MANDATORY)
    private long usedMemory;
    @Probe(level = MANDATORY)
    private long uptime;
    @Probe
    private long startTime;

    @BeforeCollectionCycle
    private void update() {
        availableProcessors = runtime.availableProcessors();
        freeMemory = runtime.freeMemory();
        maxMemory = runtime.maxMemory();
        totalMemory = runtime.totalMemory();
        usedMemory = totalMemory - freeMemory;
        uptime = runtimeMXBean.getUptime();
        startTime = runtimeMXBean.getStartTime();
    }
}
