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

package com.hazelcast.internal.probing.sources;

import com.hazelcast.internal.probing.ProbingCycle;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.internal.probing.ProbeSource;

public final class MemoryProbeSource implements ProbeSource {

    private final MemoryStats memoryStats;

    public MemoryProbeSource(MemoryStats memoryStats) {
        this.memoryStats = memoryStats;
    }

    @Override
    public void probeNow(ProbingCycle cycle) {
        cycle.probe("memory", memoryStats);
        cycle.probe("gc", memoryStats.getGCStats());
    }

}
