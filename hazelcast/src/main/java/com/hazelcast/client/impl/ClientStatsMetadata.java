package com.hazelcast.client.impl;

/**
 * Moved from MC to OSS to deal with client statistics as map(s) on members
 * instead of just a {@link String} (so that statistics themselves are
 * integrated in new metrics system).
 */
public final class ClientStatsMetadata {
    public static final String UNKNOWN_STAT_VALUE = "UNKNOWN";

    static final String NEAR_CACHE_STAT_PREFIX = "nc.";
    static final String CACHE_NAME_PREFIX = "hz/";

    static final String CLIENT_TYPE = "clientType";
    static final String CLIENT_NAME = "clientName";
    static final String CLIENT_ADDRESS = "clientAddress";
    static final String CLIENT_VERSION = "clientVersion";
    static final String ENTERPRISE = "enterprise";
    static final String LAST_STATISTICS_COLLECTION_TIME = "lastStatisticsCollectionTime";
    static final String CLUSTER_CONNECTION_TIMESTAMP = "clusterConnectionTimestamp";
    static final String USER_EXECUTOR_QUEUE_SIZE = "executionService.userExecutorQueueSize";

    static final String OS_COMMITTED_VIRTUAL_MEMORY_SIZE = "os.committedVirtualMemorySize";
    static final String OS_FREE_PHYSICAL_MEMORY_SIZE = "os.freePhysicalMemorySize";
    static final String OS_FREE_SWAP_SPACE_SIZE = "os.freeSwapSpaceSize";
    static final String OS_MAX_FILE_DESCRIPTOR_COUNT = "os.maxFileDescriptorCount";
    static final String OS_OPEN_FILE_DESCRIPTOR_COUNT = "os.openFileDescriptorCount";
    static final String OS_PROCESS_CPU_TIME = "os.processCpuTime";
    static final String OS_SYSTEM_LOAD_AVERAGE = "os.systemLoadAverage";
    static final String OS_TOTAL_PHYSICAL_MEMORY_SIZE = "os.totalPhysicalMemorySize";
    static final String OS_TOTAL_SWAP_SPACE_SIZE = "os.totalSwapSpaceSize";

    static final String RUNTIME_AVAILABLE_PROCESSORS = "runtime.availableProcessors";
    static final String RUNTIME_FREE_MEMORY = "runtime.freeMemory";
    static final String RUNTIME_MAX_MEMORY = "runtime.maxMemory";
    static final String RUNTIME_TOTAL_MEMORY = "runtime.totalMemory";
    static final String RUNTIME_UPTIME = "runtime.uptime";
    static final String RUNTIME_USED_MEMORY = "runtime.usedMemory";

    static final String NC_CREATION_TIME = "creationTime";
    static final String NC_EVICTIONS = "evictions";
    static final String NC_HITS = "hits";
    static final String NC_MISSES = "misses";
    static final String NC_OWNED_ENTRY_COUNT = "ownedEntryCount";
    static final String NC_EXPIRATIONS = "expirations";
    static final String NC_OWNED_ENTRY_MEMORY_COST = "ownedEntryMemoryCost";
    static final String NC_LAST_PERSISTENCE_DURATION = "lastPersistenceDuration";
    static final String NC_LAST_PERSISTENCE_KEY_COUNT = "lastPersistenceKeyCount";
    static final String NC_LAST_PERSISTENCE_TIME = "lastPersistenceTime";
    static final String NC_LAST_PERSISTENCE_WRITTEN_BYTES = "lastPersistenceWrittenBytes";
    static final String NC_LAST_PERSISTENCE_FAILURE = "lastPersistenceFailure";

    private ClientStatsMetadata() {
    }
}
