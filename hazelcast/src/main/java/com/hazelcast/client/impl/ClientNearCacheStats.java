package com.hazelcast.client.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.hazelcast.internal.metrics.Probe;

import static com.hazelcast.client.impl.ClientStatsMetadata.*;
import static com.hazelcast.client.impl.ClientStatsUtil.getLongOrNull;
import static com.hazelcast.client.impl.ClientStatsUtil.splitStatName;
import static com.hazelcast.client.impl.ClientStatsUtil.unescapeSpecialCharacters;

public class ClientNearCacheStats {

	private final String name;
	@Probe
    private volatile Long creationTime;
	@Probe
	private volatile Long evictions;
	@Probe
	private volatile Long hits;
	@Probe
    private volatile Long misses;
	@Probe
    private volatile Long ownedEntryCount;
	@Probe
    private volatile Long expirations;
	@Probe
    private volatile Long ownedEntryMemoryCost;
	@Probe
    private volatile Long lastPersistenceDuration;
	@Probe
    private volatile Long lastPersistenceKeyCount;
	@Probe
    private volatile Long lastPersistenceTime;
	@Probe
    private volatile Long lastPersistenceWrittenBytes;

    public ClientNearCacheStats(String name) {
		this.name = name;
	}

	String getName() {
    	return unescapeSpecialCharacters(name.startsWith(CACHE_NAME_PREFIX) ? name.substring(CACHE_NAME_PREFIX.length()) : name);
    }
	
	String getType() {
		return name.startsWith(CACHE_NAME_PREFIX) ? "cache" : "map";
	}

    void updateFrom(Map<String, String> statMap) {
    	String prefix = NEAR_CACHE_STAT_PREFIX + name + ClientStatsUtil.STAT_NAME_PART_SEPARATOR;
    	creationTime = getLongOrNull(statMap, prefix + NC_CREATION_TIME);
    	evictions = getLongOrNull(statMap, prefix + NC_EVICTIONS);
    	hits = getLongOrNull(statMap, prefix + NC_HITS);
    	misses = getLongOrNull(statMap, prefix + NC_MISSES);
    	ownedEntryCount = getLongOrNull(statMap, prefix + NC_OWNED_ENTRY_COUNT);
    	expirations = getLongOrNull(statMap, prefix + NC_EXPIRATIONS);
    	ownedEntryMemoryCost = getLongOrNull(statMap, prefix + NC_OWNED_ENTRY_MEMORY_COST);
    	lastPersistenceDuration = getLongOrNull(statMap, prefix + NC_LAST_PERSISTENCE_DURATION);
    	lastPersistenceKeyCount = getLongOrNull(statMap, prefix + NC_LAST_PERSISTENCE_KEY_COUNT);
    	lastPersistenceTime = getLongOrNull(statMap, prefix + NC_LAST_PERSISTENCE_TIME);
    	lastPersistenceWrittenBytes = getLongOrNull(statMap, prefix + NC_LAST_PERSISTENCE_WRITTEN_BYTES);
    }

    static Set<String> getNearCacheStatsDataStructureNames(Map<String, String> statMap) {
    	Set<String> names = new HashSet<String>();
        for (String fullStatName : statMap.keySet()) {
            if (fullStatName.startsWith(NEAR_CACHE_STAT_PREFIX)) {
                List<String> parts = splitStatName(fullStatName);
                if (parts != null && parts.size() == 3) {
                	names.add(parts.get(1));
                }
            }
        }
        return names;
    }

}
