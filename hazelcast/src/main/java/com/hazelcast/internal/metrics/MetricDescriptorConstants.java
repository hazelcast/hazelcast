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

package com.hazelcast.internal.metrics;

/**
 * Class holding constants for metric names, prefixes, discriminators etc.
 * <p></p>
 *
 * IMPORTANT NOTE:
 * The constants in this file are meant to be stable and can be changed
 * between minor releases only if at all. Changing any of the constants
 * in this class might break Management Center or any metrics consumer
 * code.
 */
public final class MetricDescriptorConstants {

    // ===[CACHE]=======================================================
    public static final String CACHE_PREFIX = "cache";
    public static final String CACHE_DISCRIMINATOR_NAME = "name";
    public static final String CACHE_METRIC_CREATION_TIME = "creationTime";
    public static final String CACHE_METRIC_LAST_ACCESS_TIME = "lastAccessTime";
    public static final String CACHE_METRIC_LAST_UPDATE_TIME = "lastUpdateTime";
    public static final String CACHE_METRIC_OWNED_ENTRY_COUNT = "ownedEntryCount";
    public static final String CACHE_METRIC_CACHE_HITS = "cacheHits";
    public static final String CACHE_METRIC_CACHE_HIT_PERCENTAGE = "cacheHitPercentage";
    public static final String CACHE_METRIC_CACHE_MISSES = "cacheMisses";
    public static final String CACHE_METRIC_CACHE_MISS_PERCENTAGE = "cacheMissPercentage";
    public static final String CACHE_METRIC_CACHE_GETS = "cacheGets";
    public static final String CACHE_METRIC_CACHE_PUTS = "cachePuts";
    public static final String CACHE_METRIC_CACHE_REMOVALS = "cacheRemovals";
    public static final String CACHE_METRIC_CACHE_EVICTIONS = "cacheEvictions";
    public static final String CACHE_METRIC_AVERAGE_GET_TIME = "averageGetTime";
    public static final String CACHE_METRIC_AVERAGE_PUT_TIME = "averagePutTime";
    public static final String CACHE_METRIC_AVERAGE_REMOVAL_TIME = "averageRemovalTime";
    // ===[/CACHE]======================================================

    // ===[CLASS LOADING]===============================================
    public static final String CLASSLOADING_FULL_METRIC_LOADED_CLASSES_COUNT = "classloading.loadedClassesCount";
    public static final String CLASSLOADING_FULL_METRIC_TOTAL_LOADED_CLASSES_COUNT = "classloading.totalLoadedClassesCount";
    public static final String CLASSLOADING_FULL_METRIC_UNLOADED_CLASSES_COUNT = "classloading.unloadedClassesCount";
    // ===[/CLASS LOADING]==============================================

    // ===[CLIENT]======================================================
    public static final String CLIENT_PREFIX_ENDPOINT = "client.endpoint";
    public static final String CLIENT_PREFIX_INVOCATIONS = "invocations";
    public static final String CLIENT_PREFIX_LISTENERS = "listeners";
    public static final String CLIENT_PREFIX_MEMORY = "memory";
    public static final String CLIENT_PREFIX_MEMORY_MANAGER = "memorymanager";
    public static final String CLIENT_PREFIX_EXECUTION_SERVICE = "executionService";
    public static final String CLIENT_METRIC_ENDPOINT_MANAGER_COUNT = "count";
    public static final String CLIENT_METRIC_ENDPOINT_MANAGER_TOTAL_REGISTRATIONS = "totalRegistrations";
    public static final String CLIENT_METRIC_CONNECTION_CONNECTIONID = "connectionId";
    public static final String CLIENT_METRIC_CONNECTION_EVENT_HANDLER_COUNT = "eventHandlerCount";
    public static final String CLIENT_METRIC_CONNECTION_CLOSED_TIME = "closedTime";
    public static final String CLIENT_METRIC_INVOCATIONS_PENDING_CALLS = "pendingCalls";
    public static final String CLIENT_METRIC_INVOCATIONS_STARTED_INVOCATIONS = "startedInvocations";
    public static final String CLIENT_METRIC_INVOCATIONS_MAX_CURRENT_INVOCATIONS = "maxCurrentInvocations";
    public static final String CLIENT_METRIC_LISTENER_SERVICE_EVENT_QUEUE_SIZE = "eventQueueSize";
    public static final String CLIENT_METRIC_LISTENER_SERVICE_EVENTS_PROCESSED = "eventsProcessed";
    // ===[/CLIENT]=====================================================

    // ===[CLUSTER]=====================================================
    public static final String CLUSTER_PREFIX = "cluster";
    public static final String CLUSTER_PREFIX_CLOCK = "cluster.clock";
    public static final String CLUSTER_PREFIX_CONNECTION = "cluster.connection";
    public static final String CLUSTER_PREFIX_HEARTBEAT = "cluster.heartbeat";
    public static final String CLUSTER_DISCRIMINATOR_ENDPOINT = "endpoint";
    public static final String CLUSTER_METRIC_CLUSTER_CLOCK_MAX_CLUSTER_TIME_DIFF = "maxClusterTimeDiff";
    public static final String CLUSTER_METRIC_CLUSTER_CLOCK_CLUSTER_TIME = "clusterTime";
    public static final String CLUSTER_METRIC_CLUSTER_CLOCK_CLUSTER_TIME_DIFF = "clusterTimeDiff";
    public static final String CLUSTER_METRIC_CLUSTER_CLOCK_CLUSTER_UP_TIME = "clusterUpTime";
    public static final String CLUSTER_METRIC_CLUSTER_CLOCK_LOCAL_CLOCK_TIME = "localClockTime";
    public static final String CLUSTER_METRIC_CLUSTER_CLOCK_CLUSTER_START_TIME = "clusterStartTime";
    public static final String CLUSTER_METRIC_HEARTBEAT_MANAGER_LAST_HEARTBEAT = "lastHeartbeat";
    public static final String CLUSTER_METRIC_CLUSTER_SERVICE_SIZE = "size";
    // ===[/CLUSTER]====================================================

    // ===[CP SUBSYSTEM]================================================
    public static final String CP_PREFIX_RAFT = "raft";
    public static final String CP_PREFIX_RAFT_GROUP = "raft.group";
    public static final String CP_PREFIX_RAFT_METADATA = "raft.metadata";
    public static final String CP_DISCRIMINATOR_GROUPID = "groupId";
    public static final String CP_TAG_NAME = "name";
    public static final String CP_METRIC_METADATA_RAFT_GROUP_MANAGER_GROUPS = "groups";
    public static final String CP_METRIC_METADATA_RAFT_GROUP_MANAGER_ACTIVE_MEMBERS = "activeMembers";
    public static final String CP_METRIC_METADATA_RAFT_GROUP_MANAGER_ACTIVE_MEMBERS_COMMIT_INDEX = "activeMembersCommitIndex";
    public static final String CP_METRIC_RAFT_NODE_TERM = "term";
    public static final String CP_METRIC_RAFT_NODE_COMMIT_INDEX = "commitIndex";
    public static final String CP_METRIC_RAFT_NODE_LAST_APPLIED = "lastApplied";
    public static final String CP_METRIC_RAFT_NODE_LAST_LOG_TERM = "lastLogTerm";
    public static final String CP_METRIC_RAFT_NODE_SNAPSHOT_INDEX = "snapshotIndex";
    public static final String CP_METRIC_RAFT_NODE_LAST_LOG_INDEX = "lastLogIndex";
    public static final String CP_METRIC_RAFT_NODE_AVAILABLE_LOG_CAPACITY = "availableLogCapacity";
    public static final String CP_METRIC_RAFT_SERVICE_NODES = "nodes";
    public static final String CP_METRIC_RAFT_SERVICE_DESTROYED_GROUP_IDS = "destroyedGroupIds";
    public static final String CP_METRIC_RAFT_SERVICE_TERMINATED_RAFT_NODE_GROUP_IDS = "terminatedRaftNodeGroupIds";
    public static final String CP_METRIC_RAFT_SERVICE_MISSING_MEMBERS = "missingMembers";
    // ===[/CP SUBSYSTEM]===============================================

    // ===[EVENT]=======================================================
    public static final String EVENT_PREFIX = "event";
    public static final String EVENT_DISCRIMINATOR_SERVICE = "service";
    public static final String EVENT_METRIC_EVENT_SERVICE_THREAD_COUNT = "threadCount";
    public static final String EVENT_METRIC_EVENT_SERVICE_QUEUE_CAPACITY = "queueCapacity";
    public static final String EVENT_METRIC_EVENT_SERVICE_TOTAL_FAILURE_COUNT = "totalFailureCount";
    public static final String EVENT_METRIC_EVENT_SERVICE_REJECTED_COUNT = "rejectedCount";
    public static final String EVENT_METRIC_EVENT_SERVICE_SYNC_DELIVERY_FAILURE_COUNT = "syncDeliveryFailureCount";
    public static final String EVENT_METRIC_EVENT_SERVICE_EVENT_QUEUE_SIZE = "eventQueueSize";
    public static final String EVENT_METRIC_EVENT_SERVICE_EVENTS_PROCESSED = "eventsProcessed";
    public static final String EVENT_METRIC_EVENT_SERVICE_SEGMENT_LISTENER_COUNT = "listenerCount";
    public static final String EVENT_METRIC_EVENT_SERVICE_SEGMENT_PUBLICATION_COUNT = "publicationCount";
    // ===[/EVENT]======================================================

    // ===[EXECUTOR]====================================================
    public static final String EXECUTOR_PREFIX = "executor";
    public static final String EXECUTOR_PREFIX_INTERNAL = "executor.internal";
    public static final String EXECUTOR_PREFIX_DURABLE_INTERNAL = "executor.durable.internal";
    public static final String EXECUTOR_PREFIX_SCHEDULED_INTERNAL = "executor.scheduled.internal";
    public static final String EXECUTOR_DISCRIMINATOR_NAME = "name";
    public static final String EXECUTOR_METRIC_CREATION_TIME = "creationTime";
    public static final String EXECUTOR_METRIC_PENDING = "pending";
    public static final String EXECUTOR_METRIC_STARTED = "started";
    public static final String EXECUTOR_METRIC_COMPLETED = "completed";
    public static final String EXECUTOR_METRIC_CANCELLED = "cancelled";
    public static final String EXECUTOR_METRIC_TOTAL_START_LATENCY = "totalStartLatency";
    public static final String EXECUTOR_METRIC_TOTAL_EXECUTION_TIME = "totalExecutionTime";
    public static final String EXECUTOR_METRIC_MANAGED_EXECUTOR_SERVICE_COMPLETED_TASKS = "completedTasks";
    public static final String EXECUTOR_METRIC_MANAGED_EXECUTOR_SERVICE_MAXIMUM_POOL_SIZE = "maximumPoolSize";
    public static final String EXECUTOR_METRIC_MANAGED_EXECUTOR_SERVICE_POOL_SIZE = "poolSize";
    public static final String EXECUTOR_METRIC_MANAGED_EXECUTOR_SERVICE_QUEUE_SIZE = "queueSize";
    public static final String EXECUTOR_METRIC_MANAGED_EXECUTOR_SERVICE_REMAINING_QUEUE_CAPACITY = "remainingQueueCapacity";
    // ===[/EXECUTOR]===================================================

    // ===[SCHEDULED-EXECUTOR]====================================================
    public static final String SCHEDULED_EXECUTOR_PREFIX = "scheduledExecutor";
    // ===[/SCHEDULED-EXECUTOR]===================================================

    // ===[DURABLE-EXECUTOR]====================================================
    public static final String DURABLE_EXECUTOR_PREFIX = "durableExecutor";
    // ===[/DURABLE-EXECUTOR]===================================================

    // ===[FILE]========================================================
    public static final String FILE_PREFIX = "file.partition";
    public static final String FILE_DISCRIMINATOR_DIR = "dir";
    public static final String FILE_DISCRIMINATOR_VALUE_DIR = "user.home";
    public static final String FILE_METRIC_FREESPACE = "freeSpace";
    public static final String FILE_METRIC_TOTALSPACE = "totalSpace";
    public static final String FILE_METRIC_USABLESPACE = "usableSpace";
    // ===[/FILE]=======================================================

    // ===[FLAKE ID GENERATOR]==========================================
    public static final String FLAKE_ID_GENERATOR_PREFIX = "flakeIdGenerator";
    public static final String FLAKE_ID_METRIC_CREATION_TIME = "creationTime";
    public static final String FLAKE_ID_METRIC_BATCH_COUNT = "batchCount";
    public static final String FLAKE_ID_METRIC_ID_COUNT = "idCount";
    // ===[/FLAKE ID GENERATOR]=========================================

    // ===[GC]==========================================================
    public static final String GC_PREFIX = "gc";
    public static final String GC_METRIC_MINOR_COUNT = "minorCount";
    public static final String GC_METRIC_MINOR_TIME = "minorTime";
    public static final String GC_METRIC_MAJOR_COUNT = "majorCount";
    public static final String GC_METRIC_MAJOR_TIME = "majorTime";
    public static final String GC_METRIC_UNKNOWN_COUNT = "unknownCount";
    public static final String GC_METRIC_UNKNOWN_TIME = "unknownTime";
    // ===[/GC]=========================================================

    // ===[HD]==========================================================
    public static final String HD_METRIC_USED_MEMORY = "usedMemory";
    public static final String HD_METRIC_FORCE_EVICTION_COUNT = "forceEvictionCount";
    public static final String HD_METRIC_FORCE_EVICTED_ENTRY_COUNT = "forceEvictedEntryCount";
    public static final String HD_METRIC_ENTRY_COUNT = "entryCount";
    // ===[/HD]=========================================================

    // ===[LIST]=======================================================
    public static final String LIST_PREFIX = "list";
    public static final String LIST_METRIC_LAST_ACCESS_TIME = "lastAccessTime";
    public static final String LIST_METRIC_LAST_UPDATE_TIME = "lastUpdateTime";
    public static final String LIST_METRIC_CREATION_TIME = "creationTime";
    // ===[/LIST]======================================================

    // ===[MAP]=========================================================
    public static final String MAP_PREFIX = "map";
    public static final String MAP_PREFIX_INDEX = "map.index";
    public static final String MAP_PREFIX_NEARCACHE = "map.nearcache";
    public static final String MAP_PREFIX_ENTRY_PROCESSOR_OFFLOADABLE_EXECUTOR = "map.entry.processor.offloadable.executor";
    public static final String MAP_DISCRIMINATOR_NAME = "name";
    public static final String MAP_TAG_PARTITION = "partition";
    public static final String MAP_TAG_INDEX = "index";
    public static final String MAP_METRIC_LAST_ACCESS_TIME = "lastAccessTime";
    public static final String MAP_METRIC_LAST_UPDATE_TIME = "lastUpdateTime";
    public static final String MAP_METRIC_HITS = "hits";
    public static final String MAP_METRIC_NUMBER_OF_OTHER_OPERATIONS = "numberOfOtherOperations";
    public static final String MAP_METRIC_NUMBER_OF_EVENTS = "numberOfEvents";
    public static final String MAP_METRIC_GET_COUNT = "getCount";
    public static final String MAP_METRIC_PUT_COUNT = "putCount";
    public static final String MAP_METRIC_SET_COUNT = "setCount";
    public static final String MAP_METRIC_REMOVE_COUNT = "removeCount";
    public static final String MAP_METRIC_CREATION_TIME = "creationTime";
    public static final String MAP_METRIC_OWNED_ENTRY_COUNT = "ownedEntryCount";
    public static final String MAP_METRIC_BACKUP_ENTRY_COUNT = "backupEntryCount";
    public static final String MAP_METRIC_OWNED_ENTRY_MEMORY_COST = "ownedEntryMemoryCost";
    public static final String MAP_METRIC_BACKUP_ENTRY_MEMORY_COST = "backupEntryMemoryCost";
    public static final String MAP_METRIC_HEAP_COST = "heapCost";
    public static final String MAP_METRIC_MERKLE_TREES_COST = "merkleTreesCost";
    public static final String MAP_METRIC_LOCKED_ENTRY_COUNT = "lockedEntryCount";
    public static final String MAP_METRIC_DIRTY_ENTRY_COUNT = "dirtyEntryCount";
    public static final String MAP_METRIC_BACKUP_COUNT = "backupCount";
    public static final String MAP_METRIC_QUERY_COUNT = "queryCount";
    public static final String MAP_METRIC_INDEXED_QUERY_COUNT = "indexedQueryCount";
    public static final String MAP_METRIC_TOTAL_PUT_LATENCY = "totalPutLatency";
    public static final String MAP_METRIC_TOTAL_SET_LATENCY = "totalSetLatency";
    public static final String MAP_METRIC_TOTAL_GET_LATENCY = "totalGetLatency";
    public static final String MAP_METRIC_TOTAL_REMOVE_LATENCY = "totalRemoveLatency";
    public static final String MAP_METRIC_TOTAL_MAX_PUT_LATENCY = "totalMaxPutLatency";
    public static final String MAP_METRIC_TOTAL_MAX_SET_LATENCY = "totalMaxSetLatency";
    public static final String MAP_METRIC_TOTAL_MAX_GET_LATENCY = "totalMaxGetLatency";
    public static final String MAP_METRIC_TOTAL_MAX_REMOVE_LATENCY = "totalMaxRemoveLatency";
    public static final String MAP_METRIC_INDEX_CREATION_TIME = "creationTime";
    public static final String MAP_METRIC_INDEX_QUERY_COUNT = "queryCount";
    public static final String MAP_METRIC_INDEX_HIT_COUNT = "hitCount";
    public static final String MAP_METRIC_INDEX_AVERAGE_HIT_LATENCY = "averageHitLatency";
    public static final String MAP_METRIC_INDEX_AVERAGE_HIT_SELECTIVITY = "averageHitSelectivity";
    public static final String MAP_METRIC_INDEX_INSERT_COUNT = "insertCount";
    public static final String MAP_METRIC_INDEX_TOTAL_INSERT_LATENCY = "totalInsertLatency";
    public static final String MAP_METRIC_INDEX_UPDATE_COUNT = "updateCount";
    public static final String MAP_METRIC_INDEX_TOTAL_UPDATE_LATENCY = "totalUpdateLatency";
    public static final String MAP_METRIC_INDEX_REMOVE_COUNT = "removeCount";
    public static final String MAP_METRIC_INDEX_TOTAL_REMOVE_LATENCY = "totalRemoveLatency";
    public static final String MAP_METRIC_INDEX_MEMORY_COST = "memoryCost";
    public static final String MAP_METRIC_FULL_PARTITION_REPLICATION_COUNT = "fullPartitionReplicationCount";
    public static final String MAP_METRIC_DIFF_PARTITION_REPLICATION_COUNT = "differentialPartitionReplicationCount";
    public static final String MAP_METRIC_FULL_PARTITION_REPLICATION_RECORDS_COUNT
            = "fullPartitionReplicationRecordsCount";
    public static final String MAP_METRIC_DIFF_PARTITION_REPLICATION_RECORDS_COUNT
            = "differentialPartitionReplicationRecordsCount";
    // ===[/MAP]========================================================

    // ===[MEMORY]======================================================
    public static final String MEMORY_PREFIX = "memory";
    public static final String MEMORY_METRIC_TOTAL_PHYSICAL = "totalPhysical";
    public static final String MEMORY_METRIC_FREE_PHYSICAL = "freePhysical";
    public static final String MEMORY_METRIC_MAX_HEAP = "maxHeap";
    public static final String MEMORY_METRIC_COMMITTED_HEAP = "committedHeap";
    public static final String MEMORY_METRIC_USED_HEAP = "usedHeap";
    public static final String MEMORY_METRIC_FREE_HEAP = "freeHeap";
    public static final String MEMORY_METRIC_MAX_NATIVE = "maxNative";
    public static final String MEMORY_METRIC_COMMITTED_NATIVE = "committedNative";
    public static final String MEMORY_METRIC_USED_NATIVE = "usedNative";
    public static final String MEMORY_METRIC_FREE_NATIVE = "freeNative";
    public static final String MEMORY_METRIC_MAX_METADATA = "maxMetadata";
    public static final String MEMORY_METRIC_USED_METADATA = "usedMetadata";
    // ===[/MEMORY]=====================================================

    // ===[MEMORY MANAGER]==============================================
    public static final String MEMORY_MANAGER_PREFIX = "memorymanager";
    public static final String MEMORY_MANAGER_PREFIX_STATS = "memorymanager.stats";
    // ===[/MEMORY MANAGER]=============================================

    // ===[MIGRATION]===================================================
    public static final String MIGRATION_METRIC_MIGRATION_MANAGER_MIGRATION_ACTIVE = "migrationActive";
    public static final String MIGRATION_METRIC_LAST_REPARTITION_TIME = "lastRepartitionTime";
    public static final String MIGRATION_METRIC_PLANNED_MIGRATIONS = "plannedMigrations";
    public static final String MIGRATION_METRIC_COMPLETED_MIGRATIONS = "completedMigrations";
    public static final String MIGRATION_METRIC_TOTAL_COMPLETED_MIGRATIONS = "totalCompletedMigrations";
    public static final String MIGRATION_METRIC_ELAPSED_MIGRATION_OPERATION_TIME = "elapsedMigrationOperationTime";
    public static final String MIGRATION_METRIC_ELAPSED_DESTINATION_COMMIT_TIME = "elapsedDestinationCommitTime";
    public static final String MIGRATION_METRIC_ELAPSED_MIGRATION_TIME = "elapsedMigrationTime";
    public static final String MIGRATION_METRIC_TOTAL_ELAPSED_MIGRATION_OPERATION_TIME = "totalElapsedMigrationOperationTime";
    public static final String MIGRATION_METRIC_TOTAL_ELAPSED_DESTINATION_COMMIT_TIME = "totalElapsedDestinationCommitTime";
    public static final String MIGRATION_METRIC_TOTAL_ELAPSED_MIGRATION_TIME = "totalElapsedMigrationTime";
    // ===[/MIGRATION]==================================================

    // ===[MULTIMAP]====================================================
    public static final String MULTIMAP_PREFIX = "multiMap";
    // ===[/MULTIMAP]===================================================

    // ===[NEAR CACHE]==================================================
    public static final String NEARCACHE_PREFIX = "nearcache";
    public static final String NEARCACHE_DISCRIMINATOR_NAME = "name";
    public static final String NEARCACHE_METRIC_CREATION_TIME = "creationTime";
    public static final String NEARCACHE_METRIC_OWNED_ENTRY_COUNT = "ownedEntryCount";
    public static final String NEARCACHE_METRIC_OWNED_ENTRY_MEMORY_COST = "ownedEntryMemoryCost";
    public static final String NEARCACHE_METRIC_HITS = "hits";
    public static final String NEARCACHE_METRIC_MISSES = "misses";
    public static final String NEARCACHE_METRIC_EVICTIONS = "evictions";
    public static final String NEARCACHE_METRIC_EXPIRATIONS = "expirations";
    public static final String NEARCACHE_METRIC_INVALIDATIONS = "invalidations";
    public static final String NEARCACHE_METRIC_INVALIDATION_REQUESTS = "invalidationRequests";
    public static final String NEARCACHE_METRIC_PERSISTENCE_COUNT = "persistenceCount";
    public static final String NEARCACHE_METRIC_LAST_PERSISTENCE_TIME = "lastPersistenceTime";
    public static final String NEARCACHE_METRIC_LAST_PERSISTENCE_DURATION = "lastPersistenceDuration";
    public static final String NEARCACHE_METRIC_LAST_PERSISTENCE_WRITTEN_BYTES = "lastPersistenceWrittenBytes";
    public static final String NEARCACHE_METRIC_LAST_PERSISTENCE_KEY_COUNT = "lastPersistenceKeyCount";
    // ===[/NEAR CACHE]=================================================

    // ===[NETWORKING]==================================================
    public static final String NETWORKING_METRIC_NIO_INBOUND_PIPELINE_BYTES_READ = "bytesRead";
    public static final String NETWORKING_METRIC_NIO_INBOUND_PIPELINE_NORMAL_FRAMES_READ = "normalFramesRead";
    public static final String NETWORKING_METRIC_NIO_INBOUND_PIPELINE_PRIORITY_FRAMES_READ = "priorityFramesRead";
    public static final String NETWORKING_METRIC_NIO_INBOUND_PIPELINE_IDLE_TIME_MS = "idleTimeMs";
    public static final String NETWORKING_METRIC_NIO_NETWORKING_BYTES_SEND = "bytesSend";
    public static final String NETWORKING_METRIC_NIO_NETWORKING_BYTES_RECEIVED = "bytesReceived";
    public static final String NETWORKING_METRIC_NIO_NETWORKING_PACKETS_SEND = "packetsSend";
    public static final String NETWORKING_METRIC_NIO_NETWORKING_PACKETS_RECEIVED = "packetsReceived";
    public static final String NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_WRITE_QUEUE_SIZE = "writeQueueSize";
    public static final String NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_PRIORITY_WRITE_QUEUE_SIZE = "priorityWriteQueueSize";
    public static final String NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_BYTES_WRITTEN = "bytesWritten";
    public static final String NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_NORMAL_FRAMES_WRITTEN = "normalFramesWritten";
    public static final String NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_PRIORITY_FRAMES_WRITTEN = "priorityFramesWritten";
    public static final String NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_WRITE_QUEUE_PENDING_BYTES = "writeQueuePendingBytes";
    public static final String NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_PRIORITY_WRITE_QUEUE_PENDING_BYTES =
            "priorityWriteQueuePendingBytes";
    public static final String NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_IDLE_TIME_MILLIS = "idleTimeMillis";
    public static final String NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_SCHEDULED = "scheduled";
    public static final String NETWORKING_METRIC_NIO_PIPELINE_PROCESS_COUNT = "processCount";
    public static final String NETWORKING_METRIC_NIO_PIPELINE_OWNER_ID = "ownerId";
    public static final String NETWORKING_METRIC_NIO_PIPELINE_STARTED_MIGRATIONS = "startedMigrations";
    public static final String NETWORKING_METRIC_NIO_PIPELINE_COMPLETED_MIGRATIONS = "completedMigrations";
    public static final String NETWORKING_METRIC_NIO_PIPELINE_OPS_INTERESTED = "opsInterested";
    public static final String NETWORKING_METRIC_NIO_PIPELINE_OPS_READY = "opsReady";
    public static final String NETWORKING_METRIC_NIO_THREAD_IO_THREAD_ID = "ioThreadId";
    public static final String NETWORKING_METRIC_NIO_THREAD_BYTES_TRANSCEIVED = "bytesTransceived";
    public static final String NETWORKING_METRIC_NIO_THREAD_FRAMES_TRANSCEIVED = "framesTransceived";
    public static final String NETWORKING_METRIC_NIO_THREAD_PRIORITY_FRAMES_TRANSCEIVED = "priorityFramesTransceived";
    public static final String NETWORKING_METRIC_NIO_THREAD_PROCESS_COUNT = "processCount";
    public static final String NETWORKING_METRIC_NIO_THREAD_TASK_QUEUE_SIZE = "taskQueueSize";
    public static final String NETWORKING_METRIC_NIO_THREAD_EVENT_COUNT = "eventCount";
    public static final String NETWORKING_METRIC_NIO_THREAD_SELECTOR_IO_EXCEPTION_COUNT = "selectorIOExceptionCount";
    public static final String NETWORKING_METRIC_NIO_THREAD_COMPLETED_TASK_COUNT = "completedTaskCount";
    public static final String NETWORKING_METRIC_NIO_THREAD_SELECTOR_REBUILD_COUNT = "selectorRebuildCount";
    public static final String NETWORKING_METRIC_NIO_THREAD_IDLE_TIME_MILLIS = "idleTimeMillis";
    public static final String NETWORKING_METRIC_NIO_IO_BALANCER_IMBALANCE_DETECTED_COUNT = "imbalanceDetectedCount";
    public static final String NETWORKING_METRIC_NIO_IO_BALANCER_MIGRATION_COMPLETED_COUNT = "migrationCompletedCount";
    // ===[/NETWORKING]=================================================

    // ===[OPERATION]===================================================
    public static final String OPERATION_PREFIX = "operation";
    public static final String OPERATION_PREFIX_ADHOC = "operation.adhoc";
    public static final String OPERATION_PREFIX_GENERIC = "operation.generic";
    public static final String OPERATION_PREFIX_INVOCATIONS = "operation.invocations";
    public static final String OPERATION_PREFIX_PARKER = "operation.parker";
    public static final String OPERATION_PREFIX_PARTITION = "operation.partition";
    public static final String OPERATION_PREFIX_THREAD = "operation.thread";
    public static final String OPERATION_DISCRIMINATOR_THREAD = "thread";
    public static final String OPERATION_DISCRIMINATOR_PARTITIONID = "partitionId";
    public static final String OPERATION_DISCRIMINATOR_GENERICID = "genericId";
    public static final String OPERATION_METRIC_EXECUTOR_RUNNING_COUNT = "runningCount";
    public static final String OPERATION_METRIC_EXECUTOR_RUNNING_PARTITION_COUNT = "runningPartitionCount";
    public static final String OPERATION_METRIC_EXECUTOR_RUNNING_GENERIC_COUNT = "runningGenericCount";
    public static final String OPERATION_METRIC_EXECUTOR_QUEUE_SIZE = "queueSize";
    public static final String OPERATION_METRIC_EXECUTOR_PRIORITY_QUEUE_SIZE = "priorityQueueSize";
    public static final String OPERATION_METRIC_EXECUTOR_GENERIC_QUEUE_SIZE = "genericQueueSize";
    public static final String OPERATION_METRIC_EXECUTOR_GENERIC_PRIORITY_QUEUE_SIZE = "genericPriorityQueueSize";
    public static final String OPERATION_METRIC_EXECUTOR_COMPLETED_COUNT = "completedCount";
    public static final String OPERATION_METRIC_EXECUTOR_PARTITION_THREAD_COUNT = "partitionThreadCount";
    public static final String OPERATION_METRIC_EXECUTOR_GENERIC_THREAD_COUNT = "genericThreadCount";
    public static final String OPERATION_METRIC_THREAD_COMPLETED_TOTAL_COUNT = "completedTotalCount";
    public static final String OPERATION_METRIC_THREAD_COMPLETED_PACKET_COUNT = "completedPacketCount";
    public static final String OPERATION_METRIC_THREAD_COMPLETED_OPERATION_COUNT = "completedOperationCount";
    public static final String OPERATION_METRIC_THREAD_COMPLETED_PARTITION_SPECIFIC_RUNNABLE_COUNT =
            "completedPartitionSpecificRunnableCount";
    public static final String OPERATION_METRIC_THREAD_COMPLETED_RUNNABLE_COUNT = "completedRunnableCount";
    public static final String OPERATION_METRIC_THREAD_ERROR_COUNT = "errorCount";
    public static final String OPERATION_METRIC_THREAD_COMPLETED_OPERATION_BATCH_COUNT = "completedOperationBatchCount";
    public static final String OPERATION_METRIC_PARTITION_OPERATION_THREAD_NORMAL_PENDING_COUNT = "normalPendingCount";
    public static final String OPERATION_METRIC_PARTITION_OPERATION_THREAD_PRIORITY_PENDING_COUNT = "priorityPendingCount";
    public static final String OPERATION_METRIC_PARKER_PARK_QUEUE_COUNT = "parkQueueCount";
    public static final String OPERATION_METRIC_PARKER_TOTAL_PARKED_OPERATION_COUNT = "totalParkedOperationCount";
    public static final String OPERATION_METRIC_INBOUND_RESPONSE_HANDLER_RESPONSE_QUEUE_SIZE = "responseQueueSize";
    public static final String OPERATION_METRIC_INBOUND_RESPONSE_HANDLER_RESPONSES_NORMAL_COUNT = "responses.normalCount";
    public static final String OPERATION_METRIC_INBOUND_RESPONSE_HANDLER_RESPONSES_TIMEOUT_COUNT = "responses.timeoutCount";
    public static final String OPERATION_METRIC_INBOUND_RESPONSE_HANDLER_RESPONSES_BACKUP_COUNT = "responses.backupCount";
    public static final String OPERATION_METRIC_INBOUND_RESPONSE_HANDLER_RESPONSES_ERROR_COUNT = "responses.errorCount";
    public static final String OPERATION_METRIC_INBOUND_RESPONSE_HANDLER_RESPONSES_MISSING_COUNT = "responses.missingCount";
    public static final String OPERATION_METRIC_INVOCATION_MONITOR_BACKUP_TIMEOUTS = "backupTimeouts";
    public static final String OPERATION_METRIC_INVOCATION_MONITOR_NORMAL_TIMEOUTS = "normalTimeouts";
    public static final String OPERATION_METRIC_INVOCATION_MONITOR_HEARTBEAT_PACKETS_RECEIVED = "heartbeatPacketsReceived";
    public static final String OPERATION_METRIC_INVOCATION_MONITOR_HEARTBEAT_PACKETS_SENT = "heartbeatPacketsSent";
    public static final String OPERATION_METRIC_INVOCATION_MONITOR_DELAYED_EXECUTION_COUNT = "delayedExecutionCount";
    public static final String OPERATION_METRIC_INVOCATION_MONITOR_BACKUP_TIMEOUT_MILLIS = "backupTimeoutMillis";
    public static final String OPERATION_METRIC_INVOCATION_MONITOR_INVOCATION_TIMEOUT_MILLIS = "invocationTimeoutMillis";
    public static final String OPERATION_METRIC_INVOCATION_MONITOR_HEARTBEAT_BROADCAST_PERIOD_MILLIS =
            "heartbeatBroadcastPeriodMillis";
    public static final String OPERATION_METRIC_INVOCATION_MONITOR_INVOCATION_SCAN_PERIOD_MILLIS = "invocationScanPeriodMillis";
    public static final String OPERATION_METRIC_INVOCATION_REGISTRY_INVOCATIONS_USED_PERCENTAGE = "invocations.usedPercentage";
    public static final String OPERATION_METRIC_INVOCATION_REGISTRY_INVOCATIONS_LAST_CALL_ID = "invocations.lastCallId";
    public static final String OPERATION_METRIC_INVOCATION_REGISTRY_INVOCATIONS_PENDING = "invocations.pending";
    public static final String OPERATION_METRIC_OPERATION_RUNNER_EXECUTED_OPERATIONS_COUNT = "executedOperationsCount";
    public static final String OPERATION_METRIC_OPERATION_SERVICE_ASYNC_OPERATIONS = "asyncOperations";
    public static final String OPERATION_METRIC_OPERATION_SERVICE_TIMEOUT_COUNT = "operationTimeoutCount";
    public static final String OPERATION_METRIC_OPERATION_SERVICE_CALL_TIMEOUT_COUNT = "callTimeoutCount";
    public static final String OPERATION_METRIC_OPERATION_SERVICE_RETRY_COUNT = "retryCount";
    public static final String OPERATION_METRIC_OPERATION_SERVICE_FAILED_BACKUPS = "failedBackups";
    // ===[/OPERATION]==================================================

    // ===[OS]==========================================================
    public static final String OS_FULL_METRIC_COMMITTED_VIRTUAL_MEMORY_SIZE = "os.committedVirtualMemorySize";
    public static final String OS_FULL_METRIC_FREE_PHYSICAL_MEMORY_SIZE = "os.freePhysicalMemorySize";
    public static final String OS_FULL_METRIC_FREE_SWAP_SPACE_SIZE = "os.freeSwapSpaceSize";
    public static final String OS_FULL_METRIC_PROCESS_CPU_TIME = "os.processCpuTime";
    public static final String OS_FULL_METRIC_TOTAL_PHYSICAL_MEMORY_SIZE = "os.totalPhysicalMemorySize";
    public static final String OS_FULL_METRIC_TOTAL_SWAP_SPACE_SIZE = "os.totalSwapSpaceSize";
    public static final String OS_FULL_METRIC_MAX_FILE_DESCRIPTOR_COUNT = "os.maxFileDescriptorCount";
    public static final String OS_FULL_METRIC_OPEN_FILE_DESCRIPTOR_COUNT = "os.openFileDescriptorCount";
    public static final String OS_FULL_METRIC_PROCESS_CPU_LOAD = "os.processCpuLoad";
    public static final String OS_FULL_METRIC_SYSTEM_CPU_LOAD = "os.systemCpuLoad";
    public static final String OS_FULL_METRIC_SYSTEM_LOAD_AVERAGE = "os.systemLoadAverage";
    // ===[/OS]=========================================================

    // ===[PARTITIONS]==================================================
    public static final String PARTITIONS_PREFIX = "partitions";
    public static final String PARTITIONS_METRIC_PARTITION_SERVICE_MAX_BACKUP_COUNT = "maxBackupCount";
    public static final String PARTITIONS_METRIC_PARTITION_SERVICE_MIGRATION_QUEUE_SIZE = "migrationQueueSize";
    public static final String PARTITIONS_METRIC_PARTITION_REPLICA_MANAGER_REPLICA_SYNC_SEMAPHORE = "replicaSyncSemaphore";
    public static final String PARTITIONS_METRIC_PARTITION_REPLICA_MANAGER_SYNC_REQUEST_COUNTER = "replicaSyncRequestsCounter";
    public static final String PARTITIONS_METRIC_PARTITION_REPLICA_STATE_MANAGER_PARTITION_COUNT = "partitionCount";
    public static final String PARTITIONS_METRIC_PARTITION_REPLICA_STATE_MANAGER_LOCAL_PARTITION_COUNT = "localPartitionCount";
    public static final String PARTITIONS_METRIC_PARTITION_REPLICA_STATE_MANAGER_ACTIVE_PARTITION_COUNT = "activePartitionCount";
    public static final String PARTITIONS_METRIC_PARTITION_REPLICA_STATE_MANAGER_VERSION = "stateVersion";
    public static final String PARTITIONS_METRIC_PARTITION_REPLICA_STATE_MANAGER_STAMP = "stateStamp";
    public static final String PARTITIONS_METRIC_PARTITION_REPLICA_STATE_MANAGER_MEMBER_GROUP_SIZE = "memberGroupsSize";
    // ===[/PARTITIONS]=================================================

    // ===[PERSISTENCE]=================================================
    public static final String PERSISTENCE_PREFIX = "persistence";
    public static final String PERSISTENCE_METRIC_VAL_OCCUPANCY = "valOccupancy";
    public static final String PERSISTENCE_METRIC_VAL_GARBAGE = "valGarbage";
    public static final String PERSISTENCE_METRIC_TOMB_OCCUPANCY = "tombOccupancy";
    public static final String PERSISTENCE_METRIC_TOMB_GARBAGE = "tombGarbage";
    public static final String PERSISTENCE_METRIC_GC_LIVE_VALUES = "liveValues";
    public static final String PERSISTENCE_METRIC_GC_LIVE_TOMBSTONES = "liveTombstones";
    // ===[/PERSISTENCE]================================================

    // ===[PN COUNTER]==================================================
    public static final String PNCOUNTER_PREFIX = "pnCounter";
    public static final String PNCOUNTER_METRIC_CREATION_TIME = "creationTime";
    public static final String PNCOUNTER_METRIC_VALUE = "value";
    public static final String PNCOUNTER_METRIC_TOTAL_INCREMENT_OPERATION_COUNT = "totalIncrementOperationCount";
    public static final String PNCOUNTER_METRIC_TOTAL_DECREMENT_OPERATION_COUNT = "totalDecrementOperationCount";
    // ===[/PN COUNTER]==================================================

    // ===[PROXY]=======================================================
    public static final String PROXY_PREFIX = "proxy";
    public static final String PROXY_METRIC_PROXY_COUNT = "proxyCount";
    public static final String PROXY_METRIC_CREATED_COUNT = "createdCount";
    public static final String PROXY_METRIC_DESTROYED_COUNT = "destroyedCount";
    // ===[/PROXY]=======================================================

    // ===[QUEUE]=======================================================
    public static final String QUEUE_PREFIX = "queue";
    public static final String QUEUE_METRIC_EVENT_OPERATION_COUNT = "eventOperationCount";
    public static final String QUEUE_METRIC_OWNED_ITEM_COUNT = "ownedItemCount";
    public static final String QUEUE_METRIC_BACKUP_ITEM_COUNT = "backupItemCount";
    public static final String QUEUE_METRIC_MIN_AGE = "minAge";
    public static final String QUEUE_METRIC_MAX_AGE = "maxAge";
    public static final String QUEUE_METRIC_AVERAGE_AGE = "averageAge";
    public static final String QUEUE_METRIC_CREATION_TIME = "creationTime";
    public static final String QUEUE_METRIC_NUMBER_OF_OFFERS = "numberOfOffers";
    public static final String QUEUE_METRIC_NUMBER_OF_REJECTED_OFFERS = "numberOfRejectedOffers";
    public static final String QUEUE_METRIC_NUMBER_OF_POLLS = "numberOfPolls";
    public static final String QUEUE_METRIC_NUMBER_OF_EMPTY_POLLS = "numberOfEmptyPolls";
    public static final String QUEUE_METRIC_NUMBER_OF_OTHER_OPERATIONS = "numberOfOtherOperations";
    public static final String QUEUE_METRIC_NUMBER_OF_EVENTS = "numberOfEvents";
    public static final String QUEUE_METRIC_TOTAL = "total";
    // ===[/QUEUE]======================================================

    // ===[RELIABLE TOPIC]==============================================
    public static final String RELIABLE_TOPIC_PREFIX = "reliableTopic";
    // ===[/RELIABLE TOPIC]=============================================

    // ===[REPLICATED MAP]==============================================
    public static final String REPLICATED_MAP_PREFIX = "replicatedMap";
    public static final String REPLICATED_MAP_METRIC_LAST_ACCESS_TIME = "lastAccessTime";
    public static final String REPLICATED_MAP_METRIC_LAST_UPDATE_TIME = "lastUpdateTime";
    public static final String REPLICATED_MAP_METRIC_HITS = "hits";
    public static final String REPLICATED_MAP_METRIC_NUMBER_OF_OTHER_OPERATIONS = "numberOfOtherOperations";
    public static final String REPLICATED_MAP_METRIC_NUMBER_OF_EVENTS = "numberOfEvents";
    public static final String REPLICATED_MAP_METRIC_GET_COUNT = "getCount";
    public static final String REPLICATED_MAP_METRIC_PUT_COUNT = "putCount";
    public static final String REPLICATED_MAP_METRIC_REMOVE_COUNT = "removeCount";
    public static final String REPLICATED_MAP_METRIC_TOTAL_GET_LATENCIES = "totalGetLatencies";
    public static final String REPLICATED_MAP_METRIC_TOTAL_PUT_LATENCIES = "totalPutLatencies";
    public static final String REPLICATED_MAP_METRIC_TOTAL_REMOVE_LATENCIES = "totalRemoveLatencies";
    public static final String REPLICATED_MAP_MAX_GET_LATENCY = "maxGetLatency";
    public static final String REPLICATED_MAP_MAX_PUT_LATENCY = "maxPutLatency";
    public static final String REPLICATED_MAP_MAX_REMOVE_LATENCY = "maxRemoveLatency";
    public static final String REPLICATED_MAP_CREATION_TIME = "creationTime";
    public static final String REPLICATED_MAP_OWNED_ENTRY_COUNT = "ownedEntryCount";
    public static final String REPLICATED_MAP_OWNED_ENTRY_MEMORY_COST = "ownedEntryMemoryCost";
    public static final String REPLICATED_MAP_TOTAL = "total";
    // ===[/REPLICATED MAP]==============================================

    // ===[RUNTIME]=====================================================
    public static final String RUNTIME_FULL_METRIC_FREE_MEMORY = "runtime.freeMemory";
    public static final String RUNTIME_FULL_METRIC_TOTAL_MEMORY = "runtime.totalMemory";
    public static final String RUNTIME_FULL_METRIC_MAX_MEMORY = "runtime.maxMemory";
    public static final String RUNTIME_FULL_METRIC_USED_MEMORY = "runtime.usedMemory";
    public static final String RUNTIME_FULL_METRIC_AVAILABLE_PROCESSORS = "runtime.availableProcessors";
    public static final String RUNTIME_FULL_METRIC_UPTIME = "runtime.uptime";
    // ===[/RUNTIME]====================================================

    // ===[SET]=======================================================
    public static final String SET_PREFIX = "set";
    public static final String SET_METRIC_LAST_ACCESS_TIME = "lastAccessTime";
    public static final String SET_METRIC_LAST_UPDATE_TIME = "lastUpdateTime";
    public static final String SET_METRIC_CREATION_TIME = "creationTime";
    // ===[/SET]======================================================

    // ===[TCP]=========================================================
    public static final String TCP_PREFIX = "tcp";
    public static final String TCP_PREFIX_ACCEPTOR = "tcp.acceptor";
    public static final String TCP_PREFIX_BALANCER = "tcp.balancer";
    public static final String TCP_PREFIX_CONNECTION = "tcp.connection";
    public static final String TCP_PREFIX_CONNECTION_IN = "tcp.connection.in";
    public static final String TCP_PREFIX_CONNECTION_OUT = "tcp.connection.out";
    public static final String TCP_PREFIX_INPUTTHREAD = "tcp.inputThread";
    public static final String TCP_PREFIX_OUTPUTTHREAD = "tcp.outputThread";
    public static final String TCP_DISCRIMINATOR_BINDADDRESS = "bindAddress";
    public static final String TCP_DISCRIMINATOR_ENDPOINT = "endpoint";
    public static final String TCP_DISCRIMINATOR_PIPELINEID = "pipelineId";
    public static final String TCP_DISCRIMINATOR_THREAD = "thread";
    public static final String TCP_TAG_ENDPOINT = "endpoint";
    public static final String TCP_METRIC_ACCEPTOR_EVENT_COUNT = "eventCount";
    public static final String TCP_METRIC_ACCEPTOR_EXCEPTION_COUNT = "exceptionCount";
    public static final String TCP_METRIC_ACCEPTOR_SELECTOR_RECREATE_COUNT = "selectorRecreateCount";
    public static final String TCP_METRIC_ACCEPTOR_IDLE_TIME_MILLIS = "idleTimeMillis";
    public static final String TCP_METRIC_CONNECTION_CONNECTION_TYPE = "connectionType";
    public static final String TCP_METRIC_ENDPOINT_MANAGER_IN_PROGRESS_COUNT = "inProgressCount";
    public static final String TCP_METRIC_ENDPOINT_MANAGER_COUNT = "count";
    public static final String TCP_METRIC_ENDPOINT_MANAGER_ACTIVE_COUNT = "activeCount";
    public static final String TCP_METRIC_ENDPOINT_MANAGER_CONNECTION_LISTENER_COUNT = "connectionListenerCount";
    public static final String TCP_METRIC_ENDPOINT_MANAGER_MONITOR_COUNT = "monitorCount";
    public static final String TCP_METRIC_ENDPOINT_MANAGER_OPENED_COUNT = "openedCount";
    public static final String TCP_METRIC_ENDPOINT_MANAGER_CLOSED_COUNT = "closedCount";
    public static final String TCP_METRIC_ENDPOINT_MANAGER_ACCEPTED_SOCKET_COUNT = "acceptedSocketCount";
    public static final String TCP_METRIC_CLIENT_COUNT = "clientCount";
    public static final String TCP_METRIC_TEXT_COUNT = "textCount";
    // ===[/TCP]========================================================

    // ===[TOPIC]=======================================================
    public static final String TOPIC_PREFIX = "topic";
    public static final String TOPIC_METRIC_CREATION_TIME = "creationTime";
    public static final String TOPIC_METRIC_TOTAL_PUBLISHES = "totalPublishes";
    public static final String TOPIC_METRIC_TOTAL_RECEIVED_MESSAGES = "totalReceivedMessages";
    // ===[/TOPIC]=======================================================

    // ===[TRANSACTIONS]================================================
    public static final String TRANSACTIONS_PREFIX = "transactions";
    public static final String TRANSACTIONS_METRIC_START_COUNT = "startCount";
    public static final String TRANSACTIONS_METRIC_ROLLBACK_COUNT = "rollbackCount";
    public static final String TRANSACTIONS_METRIC_COMMIT_COUNT = "commitCount";
    // ===[/TRANSACTIONS]===============================================

    // ===[THREAD]======================================================
    public static final String THREAD_FULL_METRIC_THREAD_COUNT = "thread.threadCount";
    public static final String THREAD_FULL_METRIC_PEAK_THREAD_COUNT = "thread.peakThreadCount";
    public static final String THREAD_FULL_METRIC_DAEMON_THREAD_COUNT = "thread.daemonThreadCount";
    public static final String THREAD_FULL_METRIC_TOTAL_STARTED_THREAD_COUNT = "thread.totalStartedThreadCount";
    // ===[/THREAD]=====================================================

    // ===[TSTORE]======================================================
    public static final String TSTORE_HLOG_PAGE_WRITE_DURATION_AVG = "tstore.hlog.pageWriteDuration.avg";
    public static final String TSTORE_HLOG_PAGE_WRITE_DURATION_MIN = "tstore.hlog.pageWriteDuration.min";
    public static final String TSTORE_HLOG_PAGE_WRITE_DURATION_MAX = "tstore.hlog.pageWriteDuration.max";
    public static final String TSTORE_HLOG_READ_RECORD_DURATION_AVG = "tstore.hlog.readRecordDuration.avg";
    public static final String TSTORE_HLOG_READ_RECORD_DURATION_MIN = "tstore.hlog.readRecordDuration.min";
    public static final String TSTORE_HLOG_READ_RECORD_DURATION_MAX = "tstore.hlog.readRecordDuration.max";
    public static final String TSTORE_HLOG_READ_RECORD_HITS = "tstore.hlog.readRecord.hits";
    public static final String TSTORE_HLOG_READ_RECORD_MISSES = "tstore.hlog.readRecord.misses";
    public static final String TSTORE_HLOG_READ_RECORD_HIT_PERCENT = "tstore.hlog.readRecord.hit.percent";
    public static final String TSTORE_HLOG_READ_RECORD_MISS_PERCENT = "tstore.hlog.readRecord.miss.percent";
    public static final String TSTORE_HLOG_ALLOCATION_STALL_AVG = "tstore.hlog.allocation.stall.avg";
    public static final String TSTORE_HLOG_ALLOCATION_STALL_MIN = "tstore.hlog.allocation.stall.min";
    public static final String TSTORE_HLOG_ALLOCATION_STALL_MAX = "tstore.hlog.allocation.stall.max";
    public static final String TSTORE_HLOG_ALLOCATION_STALL_TOTAL = "tstore.hlog.allocation.stall.total";
    public static final String TSTORE_HLOG_ALLOCATION_SIZE_AVG = "tstore.hlog.allocation.size.avg";
    public static final String TSTORE_HLOG_ALLOCATION_SIZE_MIN = "tstore.hlog.allocation.size.min";
    public static final String TSTORE_HLOG_ALLOCATION_SIZE_MAX = "tstore.hlog.allocation.size.max";
    public static final String TSTORE_HLOG_ALLOCATION_SIZE_TOTAL = "tstore.hlog.allocation.size.total";
    public static final String TSTORE_HLOG_WASTE_ALIGNMENT_AVG = "tstore.hlog.waste.alignment.avg";
    public static final String TSTORE_HLOG_WASTE_ALIGNMENT_MIN = "tstore.hlog.waste.alignment.min";
    public static final String TSTORE_HLOG_WASTE_ALIGNMENT_MAX = "tstore.hlog.waste.alignment.max";
    public static final String TSTORE_HLOG_WASTE_ALIGNMENT_TOTAL = "tstore.hlog.waste.alignment.total";
    public static final String TSTORE_HLOG_WASTE_PAGING_AVG = "tstore.hlog.waste.paging.avg";
    public static final String TSTORE_HLOG_WASTE_PAGING_MIN = "tstore.hlog.waste.paging.min";
    public static final String TSTORE_HLOG_WASTE_PAGING_MAX = "tstore.hlog.waste.paging.max";
    public static final String TSTORE_HLOG_WASTE_PAGING_TOTAL = "tstore.hlog.waste.paging.total";
    public static final String TSTORE_HLOG_ALLOCATION_PER_PAGE_AVG = "tstore.hlog.allocation.per.page.avg";
    public static final String TSTORE_HLOG_ALLOCATION_PER_PAGE_MIN = "tstore.hlog.allocation.per.page.min";
    public static final String TSTORE_HLOG_ALLOCATION_PER_PAGE_MAX = "tstore.hlog.allocation.per.page.max";
    public static final String TSTORE_HLOG_PAGING_FREQUENCY_AVG = "tstore.hlog.paging.frequency.avg";
    public static final String TSTORE_HLOG_PAGING_FREQUENCY_MIN = "tstore.hlog.paging.frequency.min";
    public static final String TSTORE_HLOG_PAGING_FREQUENCY_MAX = "tstore.hlog.paging.frequency.max";
    // ===[/TSTORE]=====================================================

    // ===[WAN]=========================================================
    public static final String WAN_PREFIX = "wan";
    public static final String WAN_PREFIX_CONSISTENCY_CHECK = "wan.consistencyCheck";
    public static final String WAN_PREFIX_SYNC = "wan.sync";
    public static final String WAN_DISCRIMINATOR_REPLICATION = "replication";
    public static final String WAN_TAG_CACHE = "cache";
    public static final String WAN_TAG_MAP = "map";
    public static final String WAN_TAG_PUBLISHERID = "publisherId";
    public static final String WAN_METRIC_OUTBOUND_QUEUE_SIZE = "outboundQueueSize";
    public static final String WAN_METRIC_TOTAL_PUBLISH_LATENCY = "totalPublishLatency";
    public static final String WAN_METRIC_TOTAL_PUBLISHED_EVENT_COUNT = "totalPublishedEventCount";
    public static final String WAN_METRIC_SYNC_COUNT = "syncCount";
    public static final String WAN_METRIC_UPDATE_COUNT = "updateCount";
    public static final String WAN_METRIC_REMOVE_COUNT = "removeCount";
    public static final String WAN_METRIC_DROPPED_COUNT = "droppedCount";
    public static final String WAN_METRIC_CONSISTENCY_CHECK_LAST_CHECKED_PARTITION_COUNT = "lastCheckedPartitionCount";
    public static final String WAN_METRIC_CONSISTENCY_CHECK_LAST_DIFF_PARTITION_COUNT = "lastDiffPartitionCount";
    public static final String WAN_METRIC_CONSISTENCY_CHECK_LAST_CHECKED_LEAF_COUNT = "lastCheckedLeafCount";
    public static final String WAN_METRIC_CONSISTENCY_CHECK_LAST_DIFF_LEAF_COUNT = "lastDiffLeafCount";
    public static final String WAN_METRIC_CONSISTENCY_CHECK_LAST_ENTRIES_TO_SYNC = "lastEntriesToSync";
    public static final String WAN_METRIC_FULL_SYNC_START_SYNC_NANOS = "syncStartNanos";
    public static final String WAN_METRIC_FULL_SYNC_PARTITIONS_TO_SYNC = "partitionsToSync";
    public static final String WAN_METRIC_FULL_SYNC_PARTITIONS_SYNCED = "partitionsSynced";
    public static final String WAN_METRIC_FULL_SYNC_RECORDS_SYNCED = "recordsSynced";
    public static final String WAN_METRIC_FULL_SYNC_SYNC_DURATION_NANOS = "syncDurationNanos";
    public static final String WAN_METRIC_MERKLE_SYNC_START_SYNC_NANOS = "syncStartNanos";
    public static final String WAN_METRIC_MERKLE_SYNC_PARTITIONS_TO_SYNC = "partitionsToSync";
    public static final String WAN_METRIC_MERKLE_SYNC_PARTITIONS_SYNCED = "partitionsSynced";
    public static final String WAN_METRIC_MERKLE_SYNC_RECORDS_SYNCED = "recordsSynced";
    public static final String WAN_METRIC_MERKLE_SYNC_SYNC_DURATION_NANOS = "syncDurationNanos";
    public static final String WAN_METRIC_MERKLE_SYNC_NODES_SYNCED = "nodesSynced";
    public static final String WAN_METRIC_MERKLE_SYNC_MIN_LEAF_ENTRY_COUNT = "minLeafEntryCount";
    public static final String WAN_METRIC_MERKLE_SYNC_MAX_LEAF_ENTRY_COUNT = "maxLeafEntryCount";
    public static final String WAN_METRIC_MERKLE_SYNC_AVG_ENTRIES_PER_LEAF = "avgEntriesPerLeaf";
    public static final String WAN_METRIC_MERKLE_SYNC_STD_DEV_ENTRIES_PER_LEAF = "stdDevEntriesPerLeaf";
    public static final String WAN_METRIC_ACK_DELAY_TOTAL_COUNT = "ackDelayTotalCount";
    public static final String WAN_METRIC_ACK_DELAY_TOTAL_MILLIS = "ackDelayTotalMillis";
    public static final String WAN_METRIC_ACK_DELAY_CURRENT_MILLIS = "ackDelayCurrentMillis";
    public static final String WAN_METRIC_ACK_DELAY_LAST_START = "ackDelayLastStart";
    public static final String WAN_METRIC_ACK_DELAY_LAST_END = "ackDelayLastEnd";
    public static final String WAN_QUEUE_FILL_PERCENT = "queueFillPercent";
    // ===[/WAN]========================================================

    public static final String GENERAL_DISCRIMINATOR_NAME = "name";

    private MetricDescriptorConstants() {
    }
}
