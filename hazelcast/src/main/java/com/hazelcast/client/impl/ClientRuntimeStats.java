package com.hazelcast.client.impl;

import static com.hazelcast.client.impl.ClientStatsMetadata.*;
import static com.hazelcast.client.impl.ClientStatsUtil.getIntegerOrNull;
import static com.hazelcast.client.impl.ClientStatsUtil.getLongOrNull;

import java.util.Map;

import com.hazelcast.internal.metrics.Probe;

final class ClientRuntimeStats {

	@Probe
	private volatile Integer availableProcessors;
    @Probe
    private volatile Long freeMemory;
    @Probe
    private volatile Long maxMemory;
    @Probe
    private volatile Long totalMemory;
    @Probe
    private volatile Long uptime;
    @Probe
    private volatile Long usedMemory;

    void updateFrom(Map<String, String> statMap) {
        availableProcessors = getIntegerOrNull(statMap, RUNTIME_AVAILABLE_PROCESSORS);
        freeMemory = getLongOrNull(statMap, RUNTIME_FREE_MEMORY);
        maxMemory = getLongOrNull(statMap, RUNTIME_MAX_MEMORY);
        totalMemory = getLongOrNull(statMap, RUNTIME_TOTAL_MEMORY);
        uptime = getLongOrNull(statMap, RUNTIME_UPTIME);
        usedMemory = getLongOrNull(statMap, RUNTIME_USED_MEMORY);
    }
}
