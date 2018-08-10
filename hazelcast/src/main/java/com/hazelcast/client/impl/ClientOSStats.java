package com.hazelcast.client.impl;

import java.util.Map;

import com.hazelcast.internal.metrics.Probe;

import static com.hazelcast.client.impl.ClientStatsMetadata.*;
import static com.hazelcast.client.impl.ClientStatsUtil.getDoubleOrNull;
import static com.hazelcast.client.impl.ClientStatsUtil.getLongOrNull;

final class ClientOSStats {
    
	@Probe
	private volatile Long committedVirtualMemorySize;
	@Probe
    private volatile Long freePhysicalMemorySize;
	@Probe
    private volatile Long freeSwapSpaceSize;
	@Probe
    private volatile Long maxFileDescriptorCount;
	@Probe
    private volatile Long openFileDescriptorCount;
	@Probe
    private volatile Long processCpuTime;
	@Probe
    private volatile Double systemLoadAverage;
	@Probe
    private volatile Long totalPhysicalMemorySize;
	@Probe
    private volatile Long totalSwapSpaceSize;

    public void updateFrom(Map<String, String> statMap) {
        committedVirtualMemorySize = getLongOrNull(statMap, OS_COMMITTED_VIRTUAL_MEMORY_SIZE);
        freePhysicalMemorySize = getLongOrNull(statMap, OS_FREE_PHYSICAL_MEMORY_SIZE);
        freeSwapSpaceSize = getLongOrNull(statMap, OS_FREE_SWAP_SPACE_SIZE);
        maxFileDescriptorCount = getLongOrNull(statMap, OS_MAX_FILE_DESCRIPTOR_COUNT);
        openFileDescriptorCount = getLongOrNull(statMap, OS_OPEN_FILE_DESCRIPTOR_COUNT);
        processCpuTime = getLongOrNull(statMap, OS_PROCESS_CPU_TIME);
        systemLoadAverage = getDoubleOrNull(statMap, OS_SYSTEM_LOAD_AVERAGE);
        totalPhysicalMemorySize = getLongOrNull(statMap, OS_TOTAL_PHYSICAL_MEMORY_SIZE);
        totalSwapSpaceSize = getLongOrNull(statMap, OS_TOTAL_SWAP_SPACE_SIZE);
    }

}
