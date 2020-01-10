/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
 * <p/>
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
    // ===[CLIENT]======================================================

    // ===[CLUSTER]=====================================================
    public static final String CLUSTER_PREFIX = "cluster";
    public static final String CLUSTER_PREFIX_CLOCK = "cluster.clock";
    public static final String CLUSTER_PREFIX_CONNECTION = "cluster.connection";
    public static final String CLUSTER_PREFIX_HEARTBEAT = "cluster.heartbeat";
    public static final String CLUSTER_DISCRIMINATOR_ENDPOINT = "endpoint";
    // ===[/CLUSTER]====================================================

    // ===[CP SUBSYSTEM]================================================
    public static final String CPSUBSYSTEM_PREFIX_RAFT = "raft";
    public static final String CPSUBSYSTEM_PREFIX_RAFT_GROUP = "raft.group";
    public static final String CPSUBSYSTEM_PREFIX_RAFT_METADATA = "raft.metadata";
    public static final String CPSUBSYSTEM_DISCRIMINATOR_GROUPID = "groupId";
    public static final String CPSUBSYSTEM_TAG_NAME = "name";
    // ===[/CP SUBSYSTEM]===============================================

    // ===[EVENT]=======================================================
    public static final String EVENT_PREFIX = "event";
    public static final String EVENT_DISCRIMINATOR_SERVICE = "service";
    // ===[EVENT]=======================================================

    // ===[EXECUTOR]====================================================
    public static final String EXECUTOR_PREFIX_INTERNAL = "executor.internal";
    public static final String EXECUTOR_PREFIX_DURABLE_INTERNAL = "executor.durable.internal";
    public static final String EXECUTOR_PREFIX_SCHEDULED_INTERNAL = "executor.scheduled.internal";
    public static final String EXECUTOR_DISCRIMINATOR_NAME = "name";
    // ===[/EXECUTOR]===================================================

    // ===[FILE]========================================================
    public static final String FILE_PREFIX = "file.partition";
    public static final String FILE_DISCRIMINATOR_DIR = "dir";
    public static final String FILE_DISCRIMINATOR_VALUE_DIR = "user.home";
    public static final String FILE_METRIC_FREESPACE = "freeSpace";
    public static final String FILE_METRIC_TOTALSPACE = "totalSpace";
    public static final String FILE_METRIC_USABLESPACE = "usableSpace";
    // ===[/FILE]========================================================

    // ===[GC]==========================================================
    public static final String GC_PREFIX = "gc";
    // ===[/GC]=========================================================

    // ===[INVOCATIONS]=================================================
    public static final String INVOCATIONS_PREFIX = "invocations";
    // ===[/INVOCATIONS]================================================

    // ===[HOT-RESTART]=================================================
    public static final String HOTRESTART_PREFIX = "hot-restart";
    // ===[/HOT-RESTART]================================================

    // ===[MAP]=========================================================
    public static final String MAP_PREFIX = "map";
    public static final String MAP_PREFIX_INDEX = "map.index";
    public static final String MAP_PREFIX_NEARCACHE = "map.nearcache";
    public static final String MAP_DISCRIMINATOR_NAME = "name";
    public static final String MAP_TAG_INDEX = "index";
    // ===[/MAP]========================================================

    // ===[MEMORY]======================================================
    public static final String MEMORY_PREFIX = "memory";
    // ===[/MEMORY]=====================================================

    // ===[MEMORY MANAGER]==============================================
    public static final String MEMORY_MANAGER_PREFIX = "memorymanager";
    public static final String MEMORY_MANAGER_PREFIX_STATS = "memorymanager.stats";
    // ===[/MEMORY MANAGER]=============================================

    // ===[NEAR CACHE]==================================================
    public static final String NEARCACHE_PREFIX = "nearcache";
    public static final String NEARCACHE_DISCRIMINATOR_NAME = "name";
    // ===[/NEAR CACHE]=================================================

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
    // ===[/PARTITIONS]=================================================

    // ===[PROXY]=======================================================
    public static final String PROXY_PREFIX = "proxy";
    // ===[/PROXY]=======================================================

    // ===[RUNTIME]=====================================================
    public static final String RUNTIME_FULL_METRIC_FREE_MEMORY = "runtime.freeMemory";
    public static final String RUNTIME_FULL_METRIC_TOTAL_MEMORY = "runtime.totalMemory";
    public static final String RUNTIME_FULL_METRIC_MAX_MEMORY = "runtime.maxMemory";
    public static final String RUNTIME_FULL_METRIC_USED_MEMORY = "runtime.usedMemory";
    public static final String RUNTIME_FULL_METRIC_AVAILABLE_PROCESSORS = "runtime.availableProcessors";
    public static final String RUNTIME_FULL_METRIC_UPTIME = "runtime.uptime";
    // ===[/RUNTIME]====================================================

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
    // ===[/TCP]========================================================

    // ===[TRANSACTIONS]================================================
    public static final String TRANSACTIONS_PREFIX = "transactions";
    // ===[/TRANSACTIONS]===============================================

    // ===[THREAD]======================================================
    public static final String THREAD_FULL_METRIC_THREAD_COUNT = "thread.threadCount";
    public static final String THREAD_FULL_METRIC_PEAK_THREAD_COUNT = "thread.peakThreadCount";
    public static final String THREAD_FULL_METRIC_DAEMON_THREAD_COUNT = "thread.daemonThreadCount";
    public static final String THREAD_FULL_METRIC_TOTAL_STARTED_THREAD_COUNT = "thread.totalStartedThreadCount";
    // ===[/TCP]========================================================

    // ===[WAN]=========================================================
    public static final String WAN_PREFIX = "wan";
    public static final String WAN_PREFIX_CONSISTENCY_CHECK = "wan.consistencyCheck";
    public static final String WAN_PREFIX_SYNC = "wan.sync";
    public static final String WAN_DISCRIMINATOR_REPLICATION = "replication";
    public static final String WAN_TAG_CACHE = "cache";
    public static final String WAN_TAG_MAP = "map";
    public static final String WAN_TAG_PUBLISHERID = "publisherId";
    // ===[/WAN]========================================================

    public static final String GENERAL_DISCRIMINATOR_NAME = "name";

    private MetricDescriptorConstants() {
    }
}
