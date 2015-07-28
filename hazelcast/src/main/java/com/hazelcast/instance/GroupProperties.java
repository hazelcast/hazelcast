/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.monitors.HealthMonitorLevel;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.QueryResultSizeLimiter;
import com.hazelcast.query.TruePredicate;

/**
 * The GroupProperties contain the Hazelcast properties. They can be set as an environmental variable, or
 * directly on the Config using {@link Config#setProperty(String, String)} or from the XML.
 */
public class GroupProperties {

    /**
     * Use this property to verify that Hazelcast nodes only join the cluster when their 'application' level configuration is the
     * same.
     * <p/>
     * If you have multiple machines, and you want to make sure that each machine that joins the cluster
     * has exactly the same 'application level' settings (such as settings that are not part of the Hazelcast configuration,
     * maybe some filepath). To prevent these machines with potential different application level configuration from forming
     * a cluster, you can set this property.
     * <p/>
     * You could use actual values, such as string paths, but you can also use an md5 hash. We'll give the guarantee
     * that nodes will form a cluster (become a member) only where the token is an exact match. If this token is different, the
     * member can't be started and therefore you will get the guarantee that all members in the cluster will have exactly the same
     * application validation token.
     * <p/>
     * This validation-token will be checked before member join the cluster.
     */
    public static final String PROP_APPLICATION_VALIDATION_TOKEN = "hazelcast.application.validation.token";

    public static final String PROP_HEALTH_MONITORING_LEVEL = "hazelcast.health.monitoring.level";
    public static final String PROP_HEALTH_MONITORING_DELAY_SECONDS = "hazelcast.health.monitoring.delay.seconds";

    /**
     * Use the performance monitor to see internal performance metrics. Currently this is quite
     * limited since it will only show read/write events per selector and operations executed per operation-thread. But in
     * the future, all kinds of new metrics will be added.
     * <p/>
     * The performance monitor logs all metrics into the log file.
     *
     * The default is false.
     */
    public static final String PROP_PERFORMANCE_MONITOR_ENABLED = "hazelcast.performance.monitoring.enabled";

    /**
     * The delay in seconds between monitor of the performance.
     *
     * The default is 30 seconds.
     */
    public static final String PROP_PERFORMANCE_MONITOR_DELAY_SECONDS
            = "hazelcast.performance.monitor.delay.seconds";

    /**
     * The PerformanceMonitor uses a rolling file approach to prevent eating too much disk space.
     *
     * This property sets the maximum size in MB for a single file.
     *
     * Every HazelcastInstance will get its own history of log files.
     *
     * The default is 10.
     */
    public static final String PROP_PERFORMANCE_MONITOR_MAX_ROLLED_FILE_SIZE_MB
            = "hazelcast.performance.monitor.max.rolled.file.size.mb";

    /**
     * The PerformanceMonitor uses a rolling file approach to prevent eating too much disk space.
     *
     * This property sets the maximum number of rolling files to keep on disk.
     *
     * The default is 10.
     */
    public static final String PROP_PERFORMANCE_MONITOR_MAX_ROLLED_FILE_COUNT
            = "hazelcast.performance.monitor.max.rolled.file.count";

    /**
     * If a human friendly, but more difficult to parse, output format should be selected for dumping the metrics.
     *
     * The default is true.
     */
    public static final String PROP_PERFORMANCE_MONITOR_HUMAN_FRIENDLY_FORMAT
            = "hazelcast.performance.monitor.human.friendly.format";

    public static final String PROP_VERSION_CHECK_ENABLED = "hazelcast.version.check.enabled";
    public static final String PROP_PREFER_IPv4_STACK = "hazelcast.prefer.ipv4.stack";
    public static final String PROP_IO_THREAD_COUNT = "hazelcast.io.thread.count";

    /**
     * The interval in seconds between {@link com.hazelcast.nio.tcp.iobalancer.IOBalancer IOBalancer}
     * executions. The shorter intervals will catch I/O Imbalance faster, but they will cause higher overhead.
     *
     * Please see the documentation of {@link com.hazelcast.nio.tcp.iobalancer.IOBalancer IOBalancer} for a
     * detailed explanation of the problem.
     *
     * Default value is 20 seconds. A value smaller than 1 disables the balancer.
     */
    public static final String PROP_IO_BALANCER_INTERVAL_SECONDS = "hazelcast.io.balancer.interval.seconds";
    /**
     * The number of partition threads per Member. If this is less than the number of partitions on a Member, then
     * partition operations will queue behind other operations of different partitions. The default is 4.
     */
    public static final String PROP_PARTITION_OPERATION_THREAD_COUNT = "hazelcast.operation.thread.count";
    public static final String PROP_GENERIC_OPERATION_THREAD_COUNT = "hazelcast.operation.generic.thread.count";
    public static final String PROP_EVENT_THREAD_COUNT = "hazelcast.event.thread.count";
    public static final String PROP_EVENT_QUEUE_CAPACITY = "hazelcast.event.queue.capacity";
    public static final String PROP_EVENT_QUEUE_TIMEOUT_MILLIS = "hazelcast.event.queue.timeout.millis";
    public static final String PROP_CONNECT_ALL_WAIT_SECONDS = "hazelcast.connect.all.wait.seconds";
    public static final String PROP_MEMCACHE_ENABLED = "hazelcast.memcache.enabled";
    public static final String PROP_REST_ENABLED = "hazelcast.rest.enabled";
    public static final String PROP_MAP_LOAD_CHUNK_SIZE = "hazelcast.map.load.chunk.size";
    public static final String PROP_MERGE_FIRST_RUN_DELAY_SECONDS = "hazelcast.merge.first.run.delay.seconds";
    public static final String PROP_MERGE_NEXT_RUN_DELAY_SECONDS = "hazelcast.merge.next.run.delay.seconds";
    public static final String PROP_OPERATION_CALL_TIMEOUT_MILLIS = "hazelcast.operation.call.timeout.millis";

    /**
     * If an operation has backups and the backups don't complete in time, then some cleanup logic can be executed. This
     * property specifies that timeout for backups to complete.
     */
    public static final String PROP_OPERATION_BACKUP_TIMEOUT_MILLIS = "hazelcast.operation.backup.timeout.millis";

    public static final String PROP_SOCKET_BIND_ANY = "hazelcast.socket.bind.any";
    public static final String PROP_SOCKET_SERVER_BIND_ANY = "hazelcast.socket.server.bind.any";
    public static final String PROP_SOCKET_CLIENT_BIND_ANY = "hazelcast.socket.client.bind.any";
    public static final String PROP_SOCKET_CLIENT_BIND = "hazelcast.socket.client.bind";
    /**
     * The number of threads that the client engine has available for processing requests that are not partition specific.
     * Most of the requests, such as map.put and map.get, are partition specific and will use a partition-operation-thread, but
     * there are also requests that can't be executed on a partition-specific operation-thread, such as multimap.contain(value),
     * because they need to access all partitions on a given member.
     */
    public static final String PROP_CLIENT_ENGINE_THREAD_COUNT = "hazelcast.clientengine.thread.count";
    public static final String PROP_SOCKET_RECEIVE_BUFFER_SIZE = "hazelcast.socket.receive.buffer.size";
    public static final String PROP_SOCKET_SEND_BUFFER_SIZE = "hazelcast.socket.send.buffer.size";

    /**
     * Overrides receive buffer size for connections opened by clients.
     *
     * Hazelcast creates all connections with receive buffer size set according to #PROP_SOCKET_RECEIVE_BUFFER_SIZE.
     * When it detects a connection was opened by a client then it adjusts receive buffer size
     * according to this property.
     *
     * Size is in kilobytes.
     * Default: -1; Same as receive buffer size for connections opened by members.
     *
     */
    public static final String PROP_SOCKET_CLIENT_RECEIVE_BUFFER_SIZE = "hazelcast.socket.client.receive.buffer.size";

    /**
     * Overrides send buffer size for connections opened by clients.
     *
     * Hazelcast creates all connections with send buffer size set according to #PROP_SOCKET_SEND_BUFFER_SIZE.
     * When it detects a connection was opened by a client then it adjusts send buffer size
     * according to this property.
     *
     * Size is in kilobytes.
     * Default: -1; Same as receive buffer size for connections opened by members.
     *
     */
    public static final String PROP_SOCKET_CLIENT_SEND_BUFFER_SIZE = "hazelcast.socket.client.send.buffer.size";

    public static final String PROP_SOCKET_LINGER_SECONDS = "hazelcast.socket.linger.seconds";
    public static final String PROP_SOCKET_CONNECT_TIMEOUT_SECONDS = "hazelcast.socket.connect.timeout.seconds";
    public static final String PROP_SOCKET_KEEP_ALIVE = "hazelcast.socket.keep.alive";
    public static final String PROP_SOCKET_NO_DELAY = "hazelcast.socket.no.delay";
    public static final String PROP_SHUTDOWNHOOK_ENABLED = "hazelcast.shutdownhook.enabled";
    public static final String PROP_WAIT_SECONDS_BEFORE_JOIN = "hazelcast.wait.seconds.before.join";
    public static final String PROP_MAX_WAIT_SECONDS_BEFORE_JOIN = "hazelcast.max.wait.seconds.before.join";
    public static final String PROP_MAX_JOIN_SECONDS = "hazelcast.max.join.seconds";
    public static final String PROP_MAX_JOIN_MERGE_TARGET_SECONDS = "hazelcast.max.join.merge.target.seconds";
    public static final String PROP_HEARTBEAT_INTERVAL_SECONDS = "hazelcast.heartbeat.interval.seconds";
    public static final String PROP_MAX_NO_HEARTBEAT_SECONDS = "hazelcast.max.no.heartbeat.seconds";
    public static final String PROP_MAX_NO_MASTER_CONFIRMATION_SECONDS = "hazelcast.max.no.master.confirmation.seconds";
    public static final String PROP_MASTER_CONFIRMATION_INTERVAL_SECONDS
            = "hazelcast.master.confirmation.interval.seconds";
    public static final String PROP_MEMBER_LIST_PUBLISH_INTERVAL_SECONDS
            = "hazelcast.member.list.publish.interval.seconds";
    public static final String PROP_ICMP_ENABLED = "hazelcast.icmp.enabled";
    public static final String PROP_ICMP_TIMEOUT = "hazelcast.icmp.timeout";
    public static final String PROP_ICMP_TTL = "hazelcast.icmp.ttl";
    public static final String PROP_INITIAL_MIN_CLUSTER_SIZE = "hazelcast.initial.min.cluster.size";
    public static final String PROP_INITIAL_WAIT_SECONDS = "hazelcast.initial.wait.seconds";

    /**
     * The number of incremental ports, starting with port number defined in network configuration,
     * that will be used to connect to a host which is defined without a port in the TCP-IP member list
     * while a node is searching for a cluster.
     */
    public static final String PROP_TCP_JOIN_PORT_TRY_COUNT = "hazelcast.tcp.join.port.try.count";
    public static final String PROP_MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS
            = "hazelcast.map.replica.scheduled.task.delay.seconds";
    /**
     * YOu can use PROP_MAP_EXPIRY_DELAY_SECONDS to deal with some possible edge cases, such as using EntryProcessor.
     * Without this delay, you may see that an EntryProcessor running on the owner partition found a key, but
     * EntryBackupProcessor did not find it on backup, and as a result when backup promotes to owner
     * you will end up with an unprocessed key.
     */
    public static final String PROP_MAP_EXPIRY_DELAY_SECONDS = "hazelcast.map.expiry.delay.seconds";
    public static final String PROP_PARTITION_COUNT = "hazelcast.partition.count";
    public static final String PROP_LOGGING_TYPE = "hazelcast.logging.type";
    public static final String PROP_ENABLE_JMX = "hazelcast.jmx";
    public static final String PROP_ENABLE_JMX_DETAILED = "hazelcast.jmx.detailed";
    public static final String PROP_MC_MAX_VISIBLE_INSTANCE_COUNT = "hazelcast.mc.max.visible.instance.count";
    public static final String PROP_MC_MAX_VISIBLE_SLOW_OPERATION_COUNT = "hazelcast.mc.max.visible.slow.operations.count";
    public static final String PROP_MC_URL_CHANGE_ENABLED = "hazelcast.mc.url.change.enabled";
    public static final String PROP_CONNECTION_MONITOR_INTERVAL = "hazelcast.connection.monitor.interval";
    public static final String PROP_CONNECTION_MONITOR_MAX_FAULTS = "hazelcast.connection.monitor.max.faults";
    public static final String PROP_PARTITION_MIGRATION_INTERVAL = "hazelcast.partition.migration.interval";
    public static final String PROP_PARTITION_MIGRATION_TIMEOUT = "hazelcast.partition.migration.timeout";
    public static final String PROP_PARTITION_MIGRATION_ZIP_ENABLED = "hazelcast.partition.migration.zip.enabled";
    public static final String PROP_PARTITION_TABLE_SEND_INTERVAL = "hazelcast.partition.table.send.interval";
    public static final String PROP_PARTITION_BACKUP_SYNC_INTERVAL = "hazelcast.partition.backup.sync.interval";
    public static final String PROP_PARTITION_MAX_PARALLEL_REPLICATIONS
            = "hazelcast.partition.max.parallel.replications";
    public static final String PROP_PARTITIONING_STRATEGY_CLASS = "hazelcast.partitioning.strategy.class";
    public static final String PROP_GRACEFUL_SHUTDOWN_MAX_WAIT = "hazelcast.graceful.shutdown.max.wait";
    public static final String PROP_SYSTEM_LOG_ENABLED = "hazelcast.system.log.enabled";

    /**
     * Enables or disables the {@link com.hazelcast.spi.impl.operationexecutor.slowoperationdetector.SlowOperationDetector}.
     */
    public static final String PROP_SLOW_OPERATION_DETECTOR_ENABLED = "hazelcast.slow.operation.detector.enabled";

    /**
     * Defines a threshold above which a running operation in {@link com.hazelcast.spi.OperationService} is considered to be slow.
     * These operations will log a warning and will be shown in the Management Center with detailed information, e.g. stacktrace.
     */
    public static final String PROP_SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS
            = "hazelcast.slow.operation.detector.threshold.millis";


    /**
     * Defines a threshold above which a running invocation in {@link com.hazelcast.spi.OperationService} is considered
     * to be slow. Any slow invocation will be logged.
     *
     * This is an experimental feature and we do not provide any backwards compatibility guarantees on it.
     *
     * The default value is -1 indicating there is no detection.
     */
    public static final String PROP_SLOW_INVOCATION_DETECTOR_THRESHOLD_MILLIS
            = "hazelcast.slow.invocation.detector.threshold.millis";

    /**
     * This value defines the retention time of invocations in slow operation logs.
     * <p/>
     * If an invocation is older than this value, it will be purged from the log to prevent unlimited memory usage.
     * When all invocations are purged from a log, the log itself will be deleted.
     * <p/>
     * @see com.hazelcast.instance.GroupProperties#PROP_SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS
     */
    public static final String PROP_SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS
            = "hazelcast.slow.operation.detector.log.retention.seconds";

    /**
     * Purge interval for slow operation logs.
     * <p/>
     * @see com.hazelcast.instance.GroupProperties#PROP_SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS
     */
    public static final String PROP_SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS
            = "hazelcast.slow.operation.detector.log.purge.interval.seconds";

    /**
     * Defines if the stacktraces of slow operations are logged in the log file. Stacktraces will always be reported to the
     * Management Center, but by default they are not printed to keep the log size small.
     */
    public static final String PROP_SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED
            = "hazelcast.slow.operation.detector.stacktrace.logging.enabled";

    // OLD ELASTIC MEMORY PROPS
    public static final String PROP_ELASTIC_MEMORY_ENABLED = "hazelcast.elastic.memory.enabled";
    public static final String PROP_ELASTIC_MEMORY_TOTAL_SIZE = "hazelcast.elastic.memory.total.size";
    public static final String PROP_ELASTIC_MEMORY_CHUNK_SIZE = "hazelcast.elastic.memory.chunk.size";
    public static final String PROP_ELASTIC_MEMORY_SHARED_STORAGE = "hazelcast.elastic.memory.shared.storage";
    public static final String PROP_ELASTIC_MEMORY_UNSAFE_ENABLED = "hazelcast.elastic.memory.unsafe.enabled";
    public static final String PROP_ENTERPRISE_LICENSE_KEY = "hazelcast.enterprise.license.key";
    public static final String PROP_MAP_WRITE_BEHIND_QUEUE_CAPACITY = "hazelcast.map.write.behind.queue.capacity";

    /**
     * Defines event queue capacity for WAN replication. Replication Events are dropped when queue capacity is reached.
     * Having too big queue capacity may lead to OOME problems,only valid for Hazelcast Enterprise
     */
    public static final String PROP_ENTERPRISE_WAN_REP_QUEUE_CAPACITY = "hazelcast.enterprise.wanrep.queue.capacity";

    /**
     * Defines the maximum number of WAN replication events to be drained and sent to the target cluster in a batch.
     * Batches are sent in sequence to make sure of the order of events,
     * only one batch of events is sent to a target wan member at a time. After the batch is sent, an acknowledgement is awaited
     * from the target cluster.
     * If no-ack is received, the same set of events is sent again to the target cluster until the ack is received.
     * Until this process is complete, wan replication events are stored in the wan replication event queue.
     * This queue's size is limited by {@link #PROP_ENTERPRISE_WAN_REP_QUEUE_CAPACITY}. If the queued event count
     * exceeds queue capacity, no back-pressure is applied and older events in the queue will start dropping.
     * Only valid for Hazelcast Enterprise.
     */
    public static final String PROP_ENTERPRISE_WAN_REP_BATCH_SIZE = "hazelcast.enterprise.wanrep.batch.size";

    /**
     * Defines batch sending frequency in seconds,
     * When event size does not reach to {@link #PROP_ENTERPRISE_WAN_REP_BATCH_SIZE} in the given time period
     * (which is defined by {@link #PROP_ENTERPRISE_WAN_REP_BATCH_FREQUENCY_SECONDS}),
     * those events are gathered into a batch and sent to target.
     * Only valid for Hazelcast Enterprise
     */
    public static final String PROP_ENTERPRISE_WAN_REP_BATCH_FREQUENCY_SECONDS
            = "hazelcast.enterprise.wanrep.batchfrequency.seconds";

    /**
     * Defines cache invalidation event batch sending is enabled or not.
     */
    public static final String PROP_CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED
            = "hazelcast.cache.invalidation.batch.enabled";

    /**
     * Defines the maximum number of cache invalidation events to be drained and sent to the event listeners in a batch.
     */
    public static final String PROP_CACHE_INVALIDATION_MESSAGE_BATCH_SIZE
            = "hazelcast.cache.invalidation.batch.size";

    /**
     * Defines cache invalidation event batch sending frequency in seconds.
     * When event size does not reach to {@link #PROP_CACHE_INVALIDATION_MESSAGE_BATCH_SIZE} in the given time period
     * (which is defined by {@link #PROP_CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS}),
     * those events are gathered into a batch and sent to target.
     */
    public static final String PROP_CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS
            = "hazelcast.cache.invalidation.batchfrequency.seconds";

    /**
     * Defines timeout duration (in milliseconds) for a WAN replication event before retry.
     * If confirmation is not received in the period of timeout duration, event is resent to target cluster.
     * Only valid for Hazelcast Enterprise
     */
    public static final String PROP_ENTERPRISE_WAN_REP_OP_TIMEOUT_MILLIS
            = "hazelcast.enterprise.wanrep.optimeout.millis";

    public static final String PROP_CLIENT_MAX_NO_HEARTBEAT_SECONDS = "hazelcast.client.max.no.heartbeat.seconds";
    public static final String PROP_MIGRATION_MIN_DELAY_ON_MEMBER_REMOVED_SECONDS
            = "hazelcast.migration.min.delay.on.member.removed.seconds";

    /**
     * Using back pressure, you can prevent an overload of pending asynchronous backups. With a map with a
     * single asynchronous backup, producing asynchronous backups could happen at a higher rate than
     * the consumption of the backup. This can eventually lead to an OOME (especially if the backups are slow).
     * <p/>
     * With back-pressure enabled, this can't happen.
     * <p/>
     * Back pressure is implemented by making asynchronous backups operations synchronous. This prevents the internal queues from
     * overflowing because the invoker will wait for the primary and for the backups to complete. The frequency of this is
     * determined by the sync-window.
     * <p/>
     */
    public static final String PROP_BACKPRESSURE_ENABLED = "hazelcast.backpressure.enabled";

    /**
     * Controls the frequency of a BackupAwareOperation getting its async backups converted to a sync backups. This is needed
     * to prevent an accumulation of asynchronous backups and eventually running into stability issues.
     *
     * A sync window of 10 means that 1 in 10 BackupAwareOperations get their async backups convert to sync backups.
     *
     * A sync window of 1 means that every BackupAwareOperation get their async backups converted to sync backups. 1
     * is also the smallest legal value for the sync window.
     *
     * There is some randomization going on to prevent resonance. Therefore, with a sync window of n, not every Nth
     * BackupAwareOperation operation gets its async backups converted to sync.
     *
     * This property only has meaning when backpressure is enabled.
     */
    public static final String PROP_BACKPRESSURE_SYNCWINDOW = "hazelcast.backpressure.syncwindow";

    /**
     * Control the maximum timeout in millis to wait for an invocation space to be available.
     *
     * If an invocation can't be made because there are too many pending invocations, then an exponential backoff is done
     * to give the system time to deal with the backlog of invocations. This property controls how long an invocation is
     * allowed to wait before getting a {@link com.hazelcast.core.HazelcastOverloadException}.
     *
     * The value need to be equal or larger than 0.
     */
    public static final String PROP_BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS
            = "hazelcast.backpressure.backoff.timeout.millis";

    /**
     * The maximum number of concurrent invocations per partition.
     *
     * To prevent the system from overloading, HZ can apply a constraint on the number of concurrent invocations. If the maximum
     * number of concurrent invocations has been exceeded and a new invocation comes in, then an exponential back-off is applied
     * till eventually a timeout happens or there is room for the invocation.
     *
     * By default it is configured as 100. With 271 partitions, that would give (271+1)*100=27200 concurrent invocations from a
     * single member. The +1 is for generic operations. The reasons why 100 is chosen are:
     * - there can be concurrent operations that touch a lot of partitions which consume more than 1 invocation, and
     * - certain methods like those from the IExecutor or ILock are also invocations and they can be very long running.
     *
     * No promise is made for the invocations being tracked per partition, or if there is a general pool of invocations.
     */
    public static final String PROP_BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION
            = "hazelcast.backpressure.max.concurrent.invocations.per.partition";


    /**
     * Run Query Evaluations for multiple partitions in parallel.
     *
     * Each Hazelcast member evaluates query predicates using a single thread by default. In most cases the overhead of
     * inter-thread communication overweight benefit of parallel execution.
     *
     * When you have a large dataset and/or slow predicate you may benefit from parallel predicate evaluations.
     * Set to true if you are using slow predicates or have > 100,000s entries per member.
     *
     * Default: false
     *
     */
    public static final String PROP_QUERY_PREDICATE_PARALLEL_EVALUATION = "hazelcast.query.predicate.parallel.evaluation";


    /**
     * Forces the jcache provider, which can have values client or server, to force the provider type.
     * Tf not provided, the provider will be client or server, whichever is found on the classPath first respectively.
     */
    public static final String PROP_JCACHE_PROVIDER_TYPE = "hazelcast.jcache.provider.type";

    /**
     * Result size limit for query operations on maps.
     * <p/>
     * This value defines the maximum number of returned elements for a single query result. If a query exceeds this number of
     * elements, a {@link QueryResultSizeExceededException} will be thrown.
     * <p/>
     * This feature prevents an OOME if a single node is requesting the whole data set of the cluster, such as by
     * executing a query with {@link TruePredicate}. This applies internally for the {@link IMap#values()}, {@link IMap#keySet()}
     * and {@link IMap#entrySet()} methods, which are good candidates for OOME in large clusters.
     * <p/>
     * This feature depends on an equal distribution of the data on the cluster nodes to calculate the result size limit per node.
     * Therefore, there is a minimum value of {@value QueryResultSizeLimiter#MINIMUM_MAX_RESULT_LIMIT} defined in
     * {@link QueryResultSizeLimiter}. Configured values below the minimum will be increased to the minimum.
     * <p/>
     * The feature can be disabled by setting its value to <tt>-1</tt> (which is the default value).
     */
    public static final String PROP_QUERY_RESULT_SIZE_LIMIT = "hazelcast.query.result.size.limit";

    /**
     * Maximum value of local partitions to trigger local pre-check for {@link TruePredicate} query operations on maps.
     * <p/>
     * To limit the result size of a query ({@see PROP_QUERY_RESULT_SIZE_LIMIT}), a local pre-check on the requesting node can be
     * done before the query is sent to the cluster. Since this may increase the latency, the pre-check is limited to a maximum
     * number of local partitions.
     * <p/>
     * By increasing this parameter, you can prevent the execution of the query on the cluster. Increasing this parameter
     * increases the latency due to the prolonged local pre-check.
     * <p/>
     * The pre-check can be disabled by setting the value to <tt>-1</tt>.
     *
     * @see #PROP_QUERY_RESULT_SIZE_LIMIT
     */
    public static final String PROP_QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK
            = "hazelcast.query.max.local.partition.limit.for.precheck";

    public final GroupProperty CLIENT_ENGINE_THREAD_COUNT;

    public final GroupProperty PARTITION_OPERATION_THREAD_COUNT;

    public final GroupProperty GENERIC_OPERATION_THREAD_COUNT;

    public final GroupProperty EVENT_THREAD_COUNT;

    public final GroupProperty HEALTH_MONITORING_LEVEL;

    public final GroupProperty HEALTH_MONITORING_DELAY_SECONDS;

    public final GroupProperty PERFORMANCE_MONITOR_ENABLED;

    public final GroupProperty PERFORMANCE_MONITOR_DELAY_SECONDS;

    public final GroupProperty PERFORMANCE_MONITOR_MAX_ROLLED_FILE_SIZE;

    public final GroupProperty PERFORMANCE_MONITOR_MAX_ROLLED_FILE_COUNT;

    public final GroupProperty PERFORMANCE_MONITOR_HUMAN_FRIENDLY_FORMAT;

    public final GroupProperty IO_THREAD_COUNT;

    public final GroupProperty IO_BALANCER_INTERVAL_SECONDS;

    public final GroupProperty EVENT_QUEUE_CAPACITY;

    public final GroupProperty EVENT_QUEUE_TIMEOUT_MILLIS;

    public final GroupProperty PREFER_IPv4_STACK;

    public final GroupProperty CONNECT_ALL_WAIT_SECONDS;

    public final GroupProperty VERSION_CHECK_ENABLED;

    public final GroupProperty MEMCACHE_ENABLED;

    public final GroupProperty REST_ENABLED;

    public final GroupProperty MAP_LOAD_CHUNK_SIZE;

    public final GroupProperty MERGE_FIRST_RUN_DELAY_SECONDS;

    public final GroupProperty MERGE_NEXT_RUN_DELAY_SECONDS;

    public final GroupProperty OPERATION_CALL_TIMEOUT_MILLIS;

    public final GroupProperty OPERATION_BACKUP_TIMEOUT_MILLIS;

    public final GroupProperty SOCKET_SERVER_BIND_ANY;

    public final GroupProperty SOCKET_CLIENT_BIND_ANY;

    public final GroupProperty SOCKET_CLIENT_BIND;

    // number of kilobytes
    public final GroupProperty SOCKET_RECEIVE_BUFFER_SIZE;

    // number of kilobytes
    public final GroupProperty SOCKET_SEND_BUFFER_SIZE;

    // number of kilobytes
    public final GroupProperty SOCKET_CLIENT_RECEIVE_BUFFER_SIZE;

    // number of kilobytes
    public final GroupProperty SOCKET_CLIENT_SEND_BUFFER_SIZE;

    public final GroupProperty SOCKET_LINGER_SECONDS;

    public final GroupProperty SOCKET_CONNECT_TIMEOUT_SECONDS;

    public final GroupProperty SOCKET_KEEP_ALIVE;

    public final GroupProperty SOCKET_NO_DELAY;

    public final GroupProperty SHUTDOWNHOOK_ENABLED;

    public final GroupProperty WAIT_SECONDS_BEFORE_JOIN;

    public final GroupProperty MAX_WAIT_SECONDS_BEFORE_JOIN;

    public final GroupProperty MAX_JOIN_SECONDS;

    public final GroupProperty MAX_JOIN_MERGE_TARGET_SECONDS;

    public final GroupProperty MAX_NO_HEARTBEAT_SECONDS;

    public final GroupProperty HEARTBEAT_INTERVAL_SECONDS;

    public final GroupProperty MASTER_CONFIRMATION_INTERVAL_SECONDS;

    public final GroupProperty MAX_NO_MASTER_CONFIRMATION_SECONDS;

    public final GroupProperty MEMBER_LIST_PUBLISH_INTERVAL_SECONDS;

    public final GroupProperty ICMP_ENABLED;

    public final GroupProperty ICMP_TIMEOUT;

    public final GroupProperty ICMP_TTL;

    public final GroupProperty INITIAL_WAIT_SECONDS;

    public final GroupProperty INITIAL_MIN_CLUSTER_SIZE;

    public final GroupProperty TCP_JOIN_PORT_TRY_COUNT;

    public final GroupProperty MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS;

    public final GroupProperty MAP_EXPIRY_DELAY_SECONDS;

    public final GroupProperty PARTITION_COUNT;

    public final GroupProperty LOGGING_TYPE;

    public final GroupProperty ENABLE_JMX;

    public final GroupProperty ENABLE_JMX_DETAILED;

    public final GroupProperty MC_MAX_INSTANCE_COUNT;

    public final GroupProperty MC_MAX_SLOW_OPERATION_COUNT;

    public final GroupProperty MC_URL_CHANGE_ENABLED;

    public final GroupProperty CONNECTION_MONITOR_INTERVAL;

    public final GroupProperty CONNECTION_MONITOR_MAX_FAULTS;

    public final GroupProperty PARTITION_MIGRATION_INTERVAL;

    public final GroupProperty PARTITION_MIGRATION_TIMEOUT;

    public final GroupProperty PARTITION_MIGRATION_ZIP_ENABLED;

    public final GroupProperty PARTITION_TABLE_SEND_INTERVAL;

    public final GroupProperty PARTITION_BACKUP_SYNC_INTERVAL;

    public final GroupProperty PARTITION_MAX_PARALLEL_REPLICATIONS;

    public final GroupProperty PARTITIONING_STRATEGY_CLASS;

    public final GroupProperty GRACEFUL_SHUTDOWN_MAX_WAIT;

    public final GroupProperty SYSTEM_LOG_ENABLED;

    public final GroupProperty SLOW_OPERATION_DETECTOR_ENABLED;
    public final GroupProperty SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS;
    public final GroupProperty SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS;
    public final GroupProperty SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS;
    public final GroupProperty SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED;

    public final GroupProperty SLOW_INVOCATION_DETECTOR_THRESHOLD_MILLIS;

    public final GroupProperty ELASTIC_MEMORY_ENABLED;

    public final GroupProperty ELASTIC_MEMORY_TOTAL_SIZE;

    public final GroupProperty ELASTIC_MEMORY_CHUNK_SIZE;

    public final GroupProperty ELASTIC_MEMORY_SHARED_STORAGE;

    public final GroupProperty ELASTIC_MEMORY_UNSAFE_ENABLED;

    public final GroupProperty ENTERPRISE_LICENSE_KEY;

    /**
     * Setting this capacity is valid if you set
     * {@link com.hazelcast.config.MapStoreConfig#writeCoalescing} to {@code false}. Otherwise
     * its value will not be taken into account.
     * <p/>
     * The per node maximum write-behind queue capacity is the total of all write-behind queue sizes in a node,
     * including backups.
     * <p/>
     * The maximum value which can be set is {@link Integer#MAX_VALUE}
     */
    public final GroupProperty MAP_WRITE_BEHIND_QUEUE_CAPACITY;

    public final GroupProperty ENTERPRISE_WAN_REP_QUEUE_CAPACITY;
    public final GroupProperty ENTERPRISE_WAN_REP_BATCH_SIZE;
    public final GroupProperty ENTERPRISE_WAN_REP_BATCH_FREQUENCY_SECONDS;
    public final GroupProperty ENTERPRISE_WAN_REP_OP_TIMEOUT_MILLIS;

    public final GroupProperty CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED;
    public final GroupProperty CACHE_INVALIDATION_MESSAGE_BATCH_SIZE;
    public final GroupProperty CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;

    public final GroupProperty CLIENT_HEARTBEAT_TIMEOUT_SECONDS;

    public final GroupProperty MIGRATION_MIN_DELAY_ON_MEMBER_REMOVED_SECONDS;

    public final GroupProperty BACKPRESSURE_ENABLED;
    public final GroupProperty BACKPRESSURE_SYNCWINDOW;
    public final GroupProperty BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS;
    public final GroupProperty BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION;

    public final GroupProperty QUERY_RESULT_SIZE_LIMIT;
    public final GroupProperty QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK;

    public final GroupProperty QUERY_PREDICATE_PARALLEL_EVALUATION;

    public GroupProperties(Config config) {
        HEALTH_MONITORING_LEVEL
                = new GroupProperty(config, PROP_HEALTH_MONITORING_LEVEL, HealthMonitorLevel.SILENT.toString());
        HEALTH_MONITORING_DELAY_SECONDS
                = new GroupProperty(config, PROP_HEALTH_MONITORING_DELAY_SECONDS, "30");

        PERFORMANCE_MONITOR_ENABLED
                = new GroupProperty(config, PROP_PERFORMANCE_MONITOR_ENABLED, "false");
        PERFORMANCE_MONITOR_DELAY_SECONDS
                = new GroupProperty(config, PROP_PERFORMANCE_MONITOR_DELAY_SECONDS, "30");
        PERFORMANCE_MONITOR_MAX_ROLLED_FILE_SIZE
                = new GroupProperty(config, PROP_PERFORMANCE_MONITOR_MAX_ROLLED_FILE_SIZE_MB, "10");
        PERFORMANCE_MONITOR_MAX_ROLLED_FILE_COUNT
                = new GroupProperty(config, PROP_PERFORMANCE_MONITOR_MAX_ROLLED_FILE_COUNT, "10");
        PERFORMANCE_MONITOR_HUMAN_FRIENDLY_FORMAT
                = new GroupProperty(config, PROP_PERFORMANCE_MONITOR_HUMAN_FRIENDLY_FORMAT, "true");

        VERSION_CHECK_ENABLED = new GroupProperty(config, PROP_VERSION_CHECK_ENABLED, "true");
        PREFER_IPv4_STACK = new GroupProperty(config, PROP_PREFER_IPv4_STACK, "true");
        IO_THREAD_COUNT = new GroupProperty(config, PROP_IO_THREAD_COUNT, "3");
        IO_BALANCER_INTERVAL_SECONDS = new GroupProperty(config, PROP_IO_BALANCER_INTERVAL_SECONDS, "20");

        //-1 means that the value is worked out dynamically.
        PARTITION_OPERATION_THREAD_COUNT = new GroupProperty(config, PROP_PARTITION_OPERATION_THREAD_COUNT, "-1");
        GENERIC_OPERATION_THREAD_COUNT = new GroupProperty(config, PROP_GENERIC_OPERATION_THREAD_COUNT, "-1");
        EVENT_THREAD_COUNT = new GroupProperty(config, PROP_EVENT_THREAD_COUNT, "5");
        EVENT_QUEUE_CAPACITY = new GroupProperty(config, PROP_EVENT_QUEUE_CAPACITY, "1000000");
        EVENT_QUEUE_TIMEOUT_MILLIS = new GroupProperty(config, PROP_EVENT_QUEUE_TIMEOUT_MILLIS, "250");
        CLIENT_ENGINE_THREAD_COUNT = new GroupProperty(config, PROP_CLIENT_ENGINE_THREAD_COUNT, "-1");

        CONNECT_ALL_WAIT_SECONDS = new GroupProperty(config, PROP_CONNECT_ALL_WAIT_SECONDS, "120");
        MEMCACHE_ENABLED = new GroupProperty(config, PROP_MEMCACHE_ENABLED, "true");
        REST_ENABLED = new GroupProperty(config, PROP_REST_ENABLED, "true");
        MAP_LOAD_CHUNK_SIZE = new GroupProperty(config, PROP_MAP_LOAD_CHUNK_SIZE, "1000");
        MERGE_FIRST_RUN_DELAY_SECONDS = new GroupProperty(config, PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "300");
        MERGE_NEXT_RUN_DELAY_SECONDS = new GroupProperty(config, PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "120");
        OPERATION_CALL_TIMEOUT_MILLIS = new GroupProperty(config, PROP_OPERATION_CALL_TIMEOUT_MILLIS, "60000");
        OPERATION_BACKUP_TIMEOUT_MILLIS = new GroupProperty(config, PROP_OPERATION_BACKUP_TIMEOUT_MILLIS, "5000");

        final GroupProperty SOCKET_BIND_ANY = new GroupProperty(config, PROP_SOCKET_BIND_ANY, "true");
        SOCKET_SERVER_BIND_ANY = new GroupProperty(config, PROP_SOCKET_SERVER_BIND_ANY, SOCKET_BIND_ANY);
        SOCKET_CLIENT_BIND_ANY = new GroupProperty(config, PROP_SOCKET_CLIENT_BIND_ANY, SOCKET_BIND_ANY);
        SOCKET_CLIENT_BIND = new GroupProperty(config, PROP_SOCKET_CLIENT_BIND, "true");
        SOCKET_RECEIVE_BUFFER_SIZE = new GroupProperty(config, PROP_SOCKET_RECEIVE_BUFFER_SIZE, "32");
        SOCKET_SEND_BUFFER_SIZE = new GroupProperty(config, PROP_SOCKET_SEND_BUFFER_SIZE, "32");
        SOCKET_CLIENT_RECEIVE_BUFFER_SIZE = new GroupProperty(config, PROP_SOCKET_CLIENT_RECEIVE_BUFFER_SIZE, "-1");
        SOCKET_CLIENT_SEND_BUFFER_SIZE = new GroupProperty(config, PROP_SOCKET_CLIENT_SEND_BUFFER_SIZE, "-1");
        SOCKET_LINGER_SECONDS = new GroupProperty(config, PROP_SOCKET_LINGER_SECONDS, "0");
        SOCKET_CONNECT_TIMEOUT_SECONDS = new GroupProperty(config, PROP_SOCKET_CONNECT_TIMEOUT_SECONDS, "0");
        SOCKET_KEEP_ALIVE = new GroupProperty(config, PROP_SOCKET_KEEP_ALIVE, "true");
        SOCKET_NO_DELAY = new GroupProperty(config, PROP_SOCKET_NO_DELAY, "true");
        SHUTDOWNHOOK_ENABLED = new GroupProperty(config, PROP_SHUTDOWNHOOK_ENABLED, "true");
        WAIT_SECONDS_BEFORE_JOIN = new GroupProperty(config, PROP_WAIT_SECONDS_BEFORE_JOIN, "5");
        MAX_WAIT_SECONDS_BEFORE_JOIN = new GroupProperty(config, PROP_MAX_WAIT_SECONDS_BEFORE_JOIN, "20");
        MAX_JOIN_SECONDS = new GroupProperty(config, PROP_MAX_JOIN_SECONDS, "300");
        MAX_JOIN_MERGE_TARGET_SECONDS = new GroupProperty(config, PROP_MAX_JOIN_MERGE_TARGET_SECONDS, "20");
        HEARTBEAT_INTERVAL_SECONDS = new GroupProperty(config, PROP_HEARTBEAT_INTERVAL_SECONDS, "5");
        MAX_NO_HEARTBEAT_SECONDS = new GroupProperty(config, PROP_MAX_NO_HEARTBEAT_SECONDS, "300");
        MASTER_CONFIRMATION_INTERVAL_SECONDS
                = new GroupProperty(config, PROP_MASTER_CONFIRMATION_INTERVAL_SECONDS, "30");
        MAX_NO_MASTER_CONFIRMATION_SECONDS = new GroupProperty(config, PROP_MAX_NO_MASTER_CONFIRMATION_SECONDS, "300");
        MEMBER_LIST_PUBLISH_INTERVAL_SECONDS
                = new GroupProperty(config, PROP_MEMBER_LIST_PUBLISH_INTERVAL_SECONDS, "300");
        ICMP_ENABLED = new GroupProperty(config, PROP_ICMP_ENABLED, "false");
        ICMP_TIMEOUT = new GroupProperty(config, PROP_ICMP_TIMEOUT, "1000");
        ICMP_TTL = new GroupProperty(config, PROP_ICMP_TTL, "0");
        INITIAL_MIN_CLUSTER_SIZE = new GroupProperty(config, PROP_INITIAL_MIN_CLUSTER_SIZE, "0");
        INITIAL_WAIT_SECONDS = new GroupProperty(config, PROP_INITIAL_WAIT_SECONDS, "0");
        TCP_JOIN_PORT_TRY_COUNT = new GroupProperty(config, PROP_TCP_JOIN_PORT_TRY_COUNT, "3");
        MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS
                = new GroupProperty(config, PROP_MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS, "10");
        MAP_EXPIRY_DELAY_SECONDS = new GroupProperty(config, PROP_MAP_EXPIRY_DELAY_SECONDS, "10");
        PARTITION_COUNT = new GroupProperty(config, PROP_PARTITION_COUNT, "271");
        LOGGING_TYPE = new GroupProperty(config, PROP_LOGGING_TYPE, "jdk");
        ENABLE_JMX = new GroupProperty(config, PROP_ENABLE_JMX, "false");
        ENABLE_JMX_DETAILED = new GroupProperty(config, PROP_ENABLE_JMX_DETAILED, "false");
        MC_MAX_INSTANCE_COUNT = new GroupProperty(config, PROP_MC_MAX_VISIBLE_INSTANCE_COUNT, "100");
        MC_MAX_SLOW_OPERATION_COUNT = new GroupProperty(config, PROP_MC_MAX_VISIBLE_SLOW_OPERATION_COUNT, "10");
        MC_URL_CHANGE_ENABLED = new GroupProperty(config, PROP_MC_URL_CHANGE_ENABLED, "true");
        CONNECTION_MONITOR_INTERVAL = new GroupProperty(config, PROP_CONNECTION_MONITOR_INTERVAL, "100");
        CONNECTION_MONITOR_MAX_FAULTS = new GroupProperty(config, PROP_CONNECTION_MONITOR_MAX_FAULTS, "3");
        PARTITION_MIGRATION_INTERVAL = new GroupProperty(config, PROP_PARTITION_MIGRATION_INTERVAL, "0");
        PARTITION_MIGRATION_TIMEOUT = new GroupProperty(config, PROP_PARTITION_MIGRATION_TIMEOUT, "300");
        PARTITION_MIGRATION_ZIP_ENABLED = new GroupProperty(config, PROP_PARTITION_MIGRATION_ZIP_ENABLED, "true");
        PARTITION_TABLE_SEND_INTERVAL = new GroupProperty(config, PROP_PARTITION_TABLE_SEND_INTERVAL, "15");
        PARTITION_BACKUP_SYNC_INTERVAL = new GroupProperty(config, PROP_PARTITION_BACKUP_SYNC_INTERVAL, "30");
        PARTITION_MAX_PARALLEL_REPLICATIONS = new GroupProperty(config, PROP_PARTITION_MAX_PARALLEL_REPLICATIONS, "5");
        PARTITIONING_STRATEGY_CLASS = new GroupProperty(config, PROP_PARTITIONING_STRATEGY_CLASS, "");
        GRACEFUL_SHUTDOWN_MAX_WAIT = new GroupProperty(config, PROP_GRACEFUL_SHUTDOWN_MAX_WAIT, "600");
        SYSTEM_LOG_ENABLED = new GroupProperty(config, PROP_SYSTEM_LOG_ENABLED, "true");

        SLOW_OPERATION_DETECTOR_ENABLED
                = new GroupProperty(config, PROP_SLOW_OPERATION_DETECTOR_ENABLED, "true");
        SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS
                = new GroupProperty(config, PROP_SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS, "10000");
        SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS
                = new GroupProperty(config, PROP_SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS, "3600");
        SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS
                = new GroupProperty(config, PROP_SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS, "300");
        SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED
                = new GroupProperty(config, PROP_SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED, "false");
        SLOW_INVOCATION_DETECTOR_THRESHOLD_MILLIS
                = new GroupProperty(config, PROP_SLOW_INVOCATION_DETECTOR_THRESHOLD_MILLIS, "-1");

        ELASTIC_MEMORY_ENABLED = new GroupProperty(config, PROP_ELASTIC_MEMORY_ENABLED, "false");
        ELASTIC_MEMORY_TOTAL_SIZE = new GroupProperty(config, PROP_ELASTIC_MEMORY_TOTAL_SIZE, "128M");
        ELASTIC_MEMORY_CHUNK_SIZE = new GroupProperty(config, PROP_ELASTIC_MEMORY_CHUNK_SIZE, "1K");
        ELASTIC_MEMORY_SHARED_STORAGE = new GroupProperty(config, PROP_ELASTIC_MEMORY_SHARED_STORAGE, "false");
        ELASTIC_MEMORY_UNSAFE_ENABLED = new GroupProperty(config, PROP_ELASTIC_MEMORY_UNSAFE_ENABLED, "false");
        ENTERPRISE_LICENSE_KEY = new GroupProperty(config, PROP_ENTERPRISE_LICENSE_KEY);
        MAP_WRITE_BEHIND_QUEUE_CAPACITY
                = new GroupProperty(config, PROP_MAP_WRITE_BEHIND_QUEUE_CAPACITY, "50000");

        ENTERPRISE_WAN_REP_QUEUE_CAPACITY = new GroupProperty(config, PROP_ENTERPRISE_WAN_REP_QUEUE_CAPACITY, "100000");
        ENTERPRISE_WAN_REP_BATCH_SIZE = new GroupProperty(config, PROP_ENTERPRISE_WAN_REP_BATCH_SIZE, "50");
        ENTERPRISE_WAN_REP_BATCH_FREQUENCY_SECONDS
                = new GroupProperty(config, PROP_ENTERPRISE_WAN_REP_BATCH_FREQUENCY_SECONDS, "5");
        ENTERPRISE_WAN_REP_OP_TIMEOUT_MILLIS = new GroupProperty(config, PROP_ENTERPRISE_WAN_REP_OP_TIMEOUT_MILLIS, "60000");

        CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED
                = new GroupProperty(config, PROP_CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED, "true");
        CACHE_INVALIDATION_MESSAGE_BATCH_SIZE
                = new GroupProperty(config, PROP_CACHE_INVALIDATION_MESSAGE_BATCH_SIZE, "100");
        CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS
                = new GroupProperty(config, PROP_CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS, "10");

        CLIENT_HEARTBEAT_TIMEOUT_SECONDS = new GroupProperty(config, PROP_CLIENT_MAX_NO_HEARTBEAT_SECONDS, "300");
        MIGRATION_MIN_DELAY_ON_MEMBER_REMOVED_SECONDS
                = new GroupProperty(config, PROP_MIGRATION_MIN_DELAY_ON_MEMBER_REMOVED_SECONDS, "5");
        BACKPRESSURE_ENABLED
                = new GroupProperty(config, PROP_BACKPRESSURE_ENABLED, "false");
        BACKPRESSURE_SYNCWINDOW
                = new GroupProperty(config, PROP_BACKPRESSURE_SYNCWINDOW, "100");
        BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION
                = new GroupProperty(config, PROP_BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION, "100");
        BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS
                = new GroupProperty(config, PROP_BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS, "60000");

        QUERY_RESULT_SIZE_LIMIT = new GroupProperty(config, PROP_QUERY_RESULT_SIZE_LIMIT, "-1");
        QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK
                = new GroupProperty(config, PROP_QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK, "3");

        QUERY_PREDICATE_PARALLEL_EVALUATION
                = new GroupProperty(config, PROP_QUERY_PREDICATE_PARALLEL_EVALUATION, "false");
    }

    public static class GroupProperty {

        private final String name;
        private final String value;

        GroupProperty(Config config, String name) {
            this(config, name, (String) null);
        }

        GroupProperty(Config config, String name, GroupProperty defaultValue) {
            this(config, name, defaultValue != null ? defaultValue.getString() : null);
        }

        GroupProperty(Config config, String name, String defaultValue) {
            this.name = name;
            String configValue = (config != null) ? config.getProperty(name) : null;
            if (configValue != null) {
                value = configValue;
            } else if (System.getProperty(name) != null) {
                value = System.getProperty(name);
            } else {
                value = defaultValue;
            }
        }

        public String getName() {
            return this.name;
        }

        public String getValue() {
            return value;
        }

        public int getInteger() {
            return Integer.parseInt(this.value);
        }

        public byte getByte() {
            return Byte.parseByte(this.value);
        }

        public boolean getBoolean() {
            return Boolean.valueOf(this.value);
        }

        public float getFloat() {
            return Float.valueOf(this.value);
        }

        public String getString() {
            return value;
        }

        public long getLong() {
            return Long.parseLong(this.value);
        }

        @Override
        public String toString() {
            return "GroupProperty [name=" + this.name + ", value=" + this.value + "]";
        }
    }
}
