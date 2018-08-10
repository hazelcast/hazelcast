package com.hazelcast.client.impl;

import static com.hazelcast.client.impl.ClientStatsMetadata.*;
import static com.hazelcast.client.impl.ClientStatsUtil.getLongOrNull;
import static com.hazelcast.util.StringUtil.isNullOrEmptyAfterTrim;

import java.util.Map;

import com.hazelcast.internal.metrics.Probe;

final class ClientStats {

	private volatile String name = "?";
	@Probe
    private volatile Long clusterConnectionTimestamp;
	@Probe
    private volatile Boolean enterprise;
	@Probe
    private volatile Long lastStatisticsCollectionTime;
	@Probe
    private volatile Long userExecutorQueueSize;
	
	void updateFrom(Map<String, String> statMap) {
        name = statMap.get(CLIENT_NAME);
		if (!isNullOrEmptyAfterTrim(statMap.get(ENTERPRISE))) {
            enterprise = Boolean.valueOf(statMap.get(ENTERPRISE));
        }
        lastStatisticsCollectionTime = getLongOrNull(statMap, LAST_STATISTICS_COLLECTION_TIME);
        clusterConnectionTimestamp = getLongOrNull(statMap, CLUSTER_CONNECTION_TIMESTAMP);
        userExecutorQueueSize = getLongOrNull(statMap, USER_EXECUTOR_QUEUE_SIZE);
	}
	
	String getName() {
		return name;
	}
	
}
