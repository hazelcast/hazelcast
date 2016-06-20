/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IMap;
import com.hazelcast.internal.diagnostics.HealthMonitorLevel;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.query.QueryResultSizeLimiter;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.predicates.QueryOptimizerFactory;
import com.hazelcast.spi.annotation.PrivateApi;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkHasText;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Defines the name and default value for Hazelcast properties.
 *
 * @deprecated since 3.7. Will be removed in 3.8.
 */
@PrivateApi
public enum GroupProperty implements HazelcastProperty {

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
    APPLICATION_VALIDATION_TOKEN("hazelcast.application.validation.token"),

    /**
     * Total number of partitions in the Hazelcast cluster.
     */
    PARTITION_COUNT("hazelcast.partition.count", 271),

    /**
     * The number of partition operation handler threads per Member.
     * <p/>
     * If this is less than the number of partitions on a Member partition operations
     * will queue behind other operations of different partitions.
     * <p/>
     * The default is -1, which means that the value is determined dynamically.
     */
    PARTITION_OPERATION_THREAD_COUNT("hazelcast.operation.thread.count", -1),

    /**
     * The number of generic operation handler threads per Member.
     * <p/>
     * The default is -1, which means that the value is determined dynamically.
     */
    GENERIC_OPERATION_THREAD_COUNT("hazelcast.operation.generic.thread.count", -1),

    /**
     * The number of threads that the client engine has available for processing requests that are not partition specific.
     * Most of the requests, such as map.put and map.get, are partition specific and will use a partition-operation-thread, but
     * there are also requests that can't be executed on a partition-specific operation-thread, such as multimap.contain(value),
     * because they need to access all partitions on a given member.
     */
    CLIENT_ENGINE_THREAD_COUNT("hazelcast.clientengine.thread.count", -1),

    EVENT_THREAD_COUNT("hazelcast.event.thread.count", 5),
    EVENT_QUEUE_CAPACITY("hazelcast.event.queue.capacity", 1000000),
    EVENT_QUEUE_TIMEOUT_MILLIS("hazelcast.event.queue.timeout.millis", 250, MILLISECONDS),

    HEALTH_MONITORING_LEVEL("hazelcast.health.monitoring.level", HealthMonitorLevel.SILENT.toString()),
    HEALTH_MONITORING_DELAY_SECONDS("hazelcast.health.monitoring.delay.seconds", 20, SECONDS),

    /**
     * Use the performance monitor to see internal performance metrics. Currently this is quite
     * limited since it will only show read/write events per selector and operations executed per operation-thread. But in
     * the future, all kinds of new metrics will be added.
     * <p/>
     * The performance monitor logs all metrics into the log file.
     * <p/>
     * For more detailed information, please check the PERFORMANCE_METRICS_LEVEL.
     * <p/>
     * The default is false.
     */
    PERFORMANCE_MONITOR_ENABLED("hazelcast.performance.monitoring.enabled", false),

    /**
     * The minimum level for probes is MANDATORY, but it can be changed to INFO or DEBUG. A lower level will increase
     * memory usage (probably just a few 100KB) and provides much greater detail on what is going on inside a HazelcastInstance.
     * <p/>
     * By default only mandatory probes are being tracked
     */
    PERFORMANCE_METRICS_LEVEL("hazelcast.performance.metric.level", ProbeLevel.MANDATORY.name()),

    /**
     * The delay in seconds between monitor of the performance.
     * <p/>
     * The default is 30 seconds.
     */
    PERFORMANCE_MONITOR_DELAY_SECONDS("hazelcast.performance.monitor.delay.seconds", 30, SECONDS),

    /**
     * The PerformanceMonitor uses a rolling file approach to prevent eating too much disk space.
     * <p/>
     * This property sets the maximum size in MB for a single file.
     * <p/>
     * Every HazelcastInstance will get its own history of log files.
     * <p/>
     * The default is 10.
     */
    PERFORMANCE_MONITOR_MAX_ROLLED_FILE_SIZE_MB("hazelcast.performance.monitor.max.rolled.file.size.mb", 10),

    /**
     * The PerformanceMonitor uses a rolling file approach to prevent eating too much disk space.
     * <p/>
     * This property sets the maximum number of rolling files to keep on disk.
     * <p/>
     * The default is 10.
     */
    PERFORMANCE_MONITOR_MAX_ROLLED_FILE_COUNT("hazelcast.performance.monitor.max.rolled.file.count", 10),

    /**
     * If a human friendly, but more difficult to parse, output format should be selected for dumping the metrics.
     * <p/>
     * The default is true.
     */
    PERFORMANCE_MONITOR_HUMAN_FRIENDLY_FORMAT("hazelcast.performance.monitor.human.friendly.format", true),

    /**
     * The number of threads doing socket input and the number of threads doing socket output.
     * <p/>
     * If e.g. 3 is configured, then you get 3 threads doing input and 3 doing output. For individual control
     * check {@link #IO_INPUT_THREAD_COUNT} and {@link #IO_OUTPUT_THREAD_COUNT}.
     * <p/>
     * The default is 3 (so 6 threads).
     */
    IO_THREAD_COUNT("hazelcast.io.thread.count", 3),

    /**
     * Controls the number of socket input threads. By default it is the same as {@link #IO_THREAD_COUNT}.
     */
    IO_INPUT_THREAD_COUNT("hazelcast.io.input.thread.count", IO_THREAD_COUNT),

    /**
     * Controls the number of socket output threads. By default it is the same as {@link #IO_THREAD_COUNT}.
     */
    IO_OUTPUT_THREAD_COUNT("hazelcast.io.output.thread.count", IO_THREAD_COUNT),

    /**
     * The interval in seconds between {@link com.hazelcast.nio.tcp.nonblocking.iobalancer.IOBalancer IOBalancer}
     * executions. The shorter intervals will catch I/O Imbalance faster, but they will cause higher overhead.
     * <p/>
     * Please see the documentation of {@link com.hazelcast.nio.tcp.nonblocking.iobalancer.IOBalancer IOBalancer} for a
     * detailed explanation of the problem.
     * <p/>
     * The default is 20 seconds. A value smaller than 1 disables the balancer.
     */
    IO_BALANCER_INTERVAL_SECONDS("hazelcast.io.balancer.interval.seconds", 20, SECONDS),

    PREFER_IPv4_STACK("hazelcast.prefer.ipv4.stack", true),

    @Deprecated
    VERSION_CHECK_ENABLED("hazelcast.version.check.enabled", true),

    PHONE_HOME_ENABLED("hazelcast.phone.home.enabled", true),

    CONNECT_ALL_WAIT_SECONDS("hazelcast.connect.all.wait.seconds", 120, SECONDS),

    MEMCACHE_ENABLED("hazelcast.memcache.enabled", false),
    REST_ENABLED("hazelcast.rest.enabled", false),

    MAP_LOAD_CHUNK_SIZE("hazelcast.map.load.chunk.size", 1000),

    MERGE_FIRST_RUN_DELAY_SECONDS("hazelcast.merge.first.run.delay.seconds", 300, SECONDS),
    MERGE_NEXT_RUN_DELAY_SECONDS("hazelcast.merge.next.run.delay.seconds", 120, SECONDS),

    OPERATION_CALL_TIMEOUT_MILLIS("hazelcast.operation.call.timeout.millis", 60000, MILLISECONDS),

    /**
     * If an operation has backups and the backups don't complete in time, then some cleanup logic can be executed.
     * This property specifies that timeout for backups to complete.
     */
    OPERATION_BACKUP_TIMEOUT_MILLIS("hazelcast.operation.backup.timeout.millis", 5000, MILLISECONDS),

    SOCKET_BIND_ANY("hazelcast.socket.bind.any", true),
    SOCKET_SERVER_BIND_ANY("hazelcast.socket.server.bind.any", SOCKET_BIND_ANY),
    SOCKET_CLIENT_BIND_ANY("hazelcast.socket.client.bind.any", SOCKET_BIND_ANY),
    SOCKET_CLIENT_BIND("hazelcast.socket.client.bind", true),

    // number of kilobytes
    SOCKET_RECEIVE_BUFFER_SIZE("hazelcast.socket.receive.buffer.size", 32),

    // number of kilobytes
    SOCKET_SEND_BUFFER_SIZE("hazelcast.socket.send.buffer.size", 32),

    /**
     * Overrides receive buffer size for connections opened by clients.
     * <p/>
     * Hazelcast creates all connections with receive buffer size set according to #PROP_SOCKET_RECEIVE_BUFFER_SIZE.
     * When it detects a connection was opened by a client then it adjusts receive buffer size
     * according to this property.
     * <p/>
     * Size is in kilobytes.
     * <p/>
     * The default is -1 (same as receive buffer size for connections opened by members).
     */
    SOCKET_CLIENT_RECEIVE_BUFFER_SIZE("hazelcast.socket.client.receive.buffer.size", -1),

    /**
     * Overrides send buffer size for connections opened by clients.
     * <p/>
     * Hazelcast creates all connections with send buffer size set according to #PROP_SOCKET_SEND_BUFFER_SIZE.
     * When it detects a connection was opened by a client then it adjusts send buffer size
     * according to this property.
     * <p/>
     * Size is in kilobytes.
     * <p/>
     * The default is -1 (same as receive buffer size for connections opened by members).
     */
    SOCKET_CLIENT_SEND_BUFFER_SIZE("hazelcast.socket.client.send.buffer.size", -1),

    SOCKET_LINGER_SECONDS("hazelcast.socket.linger.seconds", 0, SECONDS),
    SOCKET_CONNECT_TIMEOUT_SECONDS("hazelcast.socket.connect.timeout.seconds", 0, SECONDS),
    SOCKET_KEEP_ALIVE("hazelcast.socket.keep.alive", true),
    SOCKET_NO_DELAY("hazelcast.socket.no.delay", true),

    SHUTDOWNHOOK_ENABLED("hazelcast.shutdownhook.enabled", true),

    WAIT_SECONDS_BEFORE_JOIN("hazelcast.wait.seconds.before.join", 5, SECONDS),
    MAX_WAIT_SECONDS_BEFORE_JOIN("hazelcast.max.wait.seconds.before.join", 20, SECONDS),
    MAX_JOIN_SECONDS("hazelcast.max.join.seconds", 300, SECONDS),
    MAX_JOIN_MERGE_TARGET_SECONDS("hazelcast.max.join.merge.target.seconds", 20, SECONDS),
    HEARTBEAT_INTERVAL_SECONDS("hazelcast.heartbeat.interval.seconds", 5, SECONDS),
    MAX_NO_HEARTBEAT_SECONDS("hazelcast.max.no.heartbeat.seconds", 300, SECONDS),
    MASTER_CONFIRMATION_INTERVAL_SECONDS("hazelcast.master.confirmation.interval.seconds", 30, SECONDS),
    MAX_NO_MASTER_CONFIRMATION_SECONDS("hazelcast.max.no.master.confirmation.seconds", 350, SECONDS),
    MEMBER_LIST_PUBLISH_INTERVAL_SECONDS("hazelcast.member.list.publish.interval.seconds", 300, SECONDS),

    CLIENT_HEARTBEAT_TIMEOUT_SECONDS("hazelcast.client.max.no.heartbeat.seconds", 300, SECONDS),
    MIGRATION_MIN_DELAY_ON_MEMBER_REMOVED_SECONDS("hazelcast.migration.min.delay.on.member.removed.seconds", 5, SECONDS),

    CLUSTER_SHUTDOWN_TIMEOUT_SECONDS("hazelcast.cluster.shutdown.timeout.seconds", 900, SECONDS),

    ICMP_ENABLED("hazelcast.icmp.enabled", false),
    ICMP_TIMEOUT("hazelcast.icmp.timeout", 1000, MILLISECONDS),
    ICMP_TTL("hazelcast.icmp.ttl", 0),

    INITIAL_MIN_CLUSTER_SIZE("hazelcast.initial.min.cluster.size", 0),
    INITIAL_WAIT_SECONDS("hazelcast.initial.wait.seconds", 0, SECONDS),

    /**
     * The number of incremental ports, starting with port number defined in network configuration,
     * that will be used to connect to a host which is defined without a port in the TCP-IP member list
     * while a node is searching for a cluster.
     */
    TCP_JOIN_PORT_TRY_COUNT("hazelcast.tcp.join.port.try.count", 3),

    MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS("hazelcast.map.replica.scheduled.task.delay.seconds", 10, SECONDS),

    /**
     * You can use MAP_EXPIRY_DELAY_SECONDS to deal with some possible edge cases, such as using EntryProcessor.
     * Without this delay, you may see that an EntryProcessor running on the owner partition found a key, but
     * EntryBackupProcessor did not find it on backup, and as a result when backup promotes to owner
     * you will end up with an unprocessed key.
     */
    MAP_EXPIRY_DELAY_SECONDS("hazelcast.map.expiry.delay.seconds", 10, SECONDS),

    LOGGING_TYPE("hazelcast.logging.type", "jdk"),

    ENABLE_JMX("hazelcast.jmx", false),
    ENABLE_JMX_DETAILED("hazelcast.jmx.detailed", false),
    JMX_UPDATE_INTERVAL_SECONDS("hazelcast.jmx.update.interval.seconds", 5, SECONDS),

    ITERATING_MAP_STATS_ENABLED("hazelcast.management.iterating.map.stats.enabled", true),

    MC_MAX_VISIBLE_INSTANCE_COUNT("hazelcast.mc.max.visible.instance.count", 100),
    MC_MAX_VISIBLE_SLOW_OPERATION_COUNT("hazelcast.mc.max.visible.slow.operations.count", 10),
    MC_URL_CHANGE_ENABLED("hazelcast.mc.url.change.enabled", true),

    CONNECTION_MONITOR_INTERVAL("hazelcast.connection.monitor.interval", 100, MILLISECONDS),
    CONNECTION_MONITOR_MAX_FAULTS("hazelcast.connection.monitor.max.faults", 3),

    PARTITION_MIGRATION_INTERVAL("hazelcast.partition.migration.interval", 0, SECONDS),
    PARTITION_MIGRATION_TIMEOUT("hazelcast.partition.migration.timeout", 300, SECONDS),
    PARTITION_MIGRATION_ZIP_ENABLED("hazelcast.partition.migration.zip.enabled", true),

    PARTITION_TABLE_SEND_INTERVAL("hazelcast.partition.table.send.interval", 15, SECONDS),
    PARTITION_BACKUP_SYNC_INTERVAL("hazelcast.partition.backup.sync.interval", 30, SECONDS),
    PARTITION_MAX_PARALLEL_REPLICATIONS("hazelcast.partition.max.parallel.replications", 5),
    PARTITIONING_STRATEGY_CLASS("hazelcast.partitioning.strategy.class", ""),

    GRACEFUL_SHUTDOWN_MAX_WAIT("hazelcast.graceful.shutdown.max.wait", 600, SECONDS),

    SYSTEM_LOG_ENABLED("hazelcast.system.log.enabled", true),

    /**
     * Enables or disables the {@link com.hazelcast.spi.impl.operationexecutor.slowoperationdetector.SlowOperationDetector}.
     */
    SLOW_OPERATION_DETECTOR_ENABLED("hazelcast.slow.operation.detector.enabled", true),

    /**
     * Defines a threshold above which a running operation in {@link com.hazelcast.spi.OperationService} is considered to be slow.
     * These operations will log a warning and will be shown in the Management Center with detailed information, e.g. stacktrace.
     */
    SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS("hazelcast.slow.operation.detector.threshold.millis", 10000, MILLISECONDS),

    /**
     * This value defines the retention time of invocations in slow operation logs.
     * <p/>
     * If an invocation is older than this value, it will be purged from the log to prevent unlimited memory usage.
     * When all invocations are purged from a log, the log itself will be deleted.
     *
     * @see #SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS
     */
    SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS("hazelcast.slow.operation.detector.log.retention.seconds", 3600, SECONDS),

    /**
     * Purge interval for slow operation logs.
     *
     * @see #SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS
     */
    SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS("hazelcast.slow.operation.detector.log.purge.interval.seconds",
            300, SECONDS),

    /**
     * Defines if the stacktraces of slow operations are logged in the log file. Stacktraces will always be reported to the
     * Management Center, but by default they are not printed to keep the log size small.
     */
    SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED("hazelcast.slow.operation.detector.stacktrace.logging.enabled", false),

    /**
     * Defines a threshold above which a running invocation in {@link com.hazelcast.spi.OperationService} is considered
     * to be slow. Any slow invocation will be logged.
     * <p/>
     * This is an experimental feature and we do not provide any backwards compatibility guarantees on it.
     * <p/>
     * The default is -1 indicating there is no detection.
     */
    SLOW_INVOCATION_DETECTOR_THRESHOLD_MILLIS("hazelcast.slow.invocation.detector.threshold.millis", -1, MILLISECONDS),

    LOCK_MAX_LEASE_TIME_SECONDS("hazelcast.lock.max.lease.time.seconds", Long.MAX_VALUE, SECONDS),

    ENTERPRISE_LICENSE_KEY("hazelcast.enterprise.license.key"),

    /**
     * Setting this capacity is valid if you set {@link com.hazelcast.config.MapStoreConfig#writeCoalescing} to {@code false}.
     * Otherwise its value will not be taken into account.
     * <p/>
     * The per node maximum write-behind queue capacity is the total of all write-behind queue sizes in a node, including backups.
     * <p/>
     * The maximum value which can be set is {@link Integer#MAX_VALUE}
     */
    MAP_WRITE_BEHIND_QUEUE_CAPACITY("hazelcast.map.write.behind.queue.capacity", 50000),

    /**
     * Defines cache invalidation event batch sending is enabled or not.
     */
    CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED("hazelcast.cache.invalidation.batch.enabled", true),

    /**
     * Defines the maximum number of cache invalidation events to be drained and sent to the event listeners in a batch.
     */
    CACHE_INVALIDATION_MESSAGE_BATCH_SIZE("hazelcast.cache.invalidation.batch.size", 100),

    /**
     * Defines the cache invalidation event batch sending frequency in seconds.
     * <p/>
     * When the number of events do not come up to {@link #CACHE_INVALIDATION_MESSAGE_BATCH_SIZE} in the given time period (which
     * is defined by this property), those events are gathered into a batch and sent to target.
     */
    CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS("hazelcast.cache.invalidation.batchfrequency.seconds", 10, SECONDS),

    /**
     * Defines near-cache invalidation event batch sending is enabled or not.
     */
    MAP_INVALIDATION_MESSAGE_BATCH_ENABLED("hazelcast.map.invalidation.batch.enabled", true),

    /**
     * Defines the maximum number of near-cache invalidation events to be drained and sent to the event near-caches in a batch.
     */
    MAP_INVALIDATION_MESSAGE_BATCH_SIZE("hazelcast.map.invalidation.batch.size", 100),

    /**
     * Defines the near-cache invalidation event batch sending frequency in seconds.
     * <p/>
     * When the number of events do not come up to {@link #MAP_INVALIDATION_MESSAGE_BATCH_SIZE} in the given time period (which
     * is defined by this property), those events are gathered into a batch and sent to target.
     */
    MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS("hazelcast.map.invalidation.batchfrequency.seconds", 10, SECONDS),

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
     */
    BACKPRESSURE_ENABLED("hazelcast.backpressure.enabled", false),

    /**
     * Controls the frequency of a BackupAwareOperation getting its async backups converted to a sync backups. This is needed
     * to prevent an accumulation of asynchronous backups and eventually running into stability issues.
     * <p/>
     * A sync window of 10 means that 1 in 10 BackupAwareOperations get their async backups convert to sync backups.
     * <p/>
     * A sync window of 1 means that every BackupAwareOperation get their async backups converted to sync backups. 1
     * is also the smallest legal value for the sync window.
     * <p/>
     * There is some randomization going on to prevent resonance. Therefore, with a sync window of n, not every Nth
     * BackupAwareOperation operation gets its async backups converted to sync.
     * <p/>
     * This property only has meaning when backpressure is enabled.
     */
    BACKPRESSURE_SYNCWINDOW("hazelcast.backpressure.syncwindow", 100),

    /**
     * Control the maximum timeout in millis to wait for an invocation space to be available.
     * <p/>
     * If an invocation can't be made because there are too many pending invocations, then an exponential backoff is done
     * to give the system time to deal with the backlog of invocations. This property controls how long an invocation is
     * allowed to wait before getting a {@link com.hazelcast.core.HazelcastOverloadException}.
     * <p/>
     * The value needs to be equal or larger than 0.
     */
    BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS("hazelcast.backpressure.backoff.timeout.millis", 60000, MILLISECONDS),

    /**
     * The maximum number of concurrent invocations per partition.
     * <p/>
     * To prevent the system from overloading, HZ can apply a constraint on the number of concurrent invocations. If the maximum
     * number of concurrent invocations has been exceeded and a new invocation comes in, then an exponential back-off is applied
     * till eventually a timeout happens or there is room for the invocation.
     * <p/>
     * By default it is configured as 100. With 271 partitions, that would give (271 + 1) * 100 = 27200 concurrent invocations
     * from a single member. The +1 is for generic operations. The reasons why 100 is chosen are:
     * - there can be concurrent operations that touch a lot of partitions which consume more than 1 invocation, and
     * - certain methods like those from the IExecutor or ILock are also invocations and they can be very long running.
     * <p/>
     * No promise is made for the invocations being tracked per partition, or if there is a general pool of invocations.
     */
    BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION("hazelcast.backpressure.max.concurrent.invocations.per.partition", 100),

    /**
     * Run Query Evaluations for multiple partitions in parallel.
     * <p/>
     * Each Hazelcast member evaluates query predicates using a single thread by default. In most cases the overhead of
     * inter-thread communication overweight benefit of parallel execution.
     * <p/>
     * When you have a large dataset and/or slow predicate you may benefit from parallel predicate evaluations.
     * Set to true if you are using slow predicates or have > 100,000s entries per member.
     * <p/>
     * The default is false.
     */
    QUERY_PREDICATE_PARALLEL_EVALUATION("hazelcast.query.predicate.parallel.evaluation", false),

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
    QUERY_RESULT_SIZE_LIMIT("hazelcast.query.result.size.limit", -1),

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
     * @see #QUERY_RESULT_SIZE_LIMIT
     */
    QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK("hazelcast.query.max.local.partition.limit.for.precheck", 3),

    /**
     * Type of Query Optimizer.
     * Valid Values:
     * <ul>
     * <li>RULES - for optimizations based on static rules</li>
     * <li>NONE - optimization are disabled</li>
     * </ul>
     * <p/>
     * Values are case sensitive
     */
    QUERY_OPTIMIZER_TYPE("hazelcast.query.optimizer.type", QueryOptimizerFactory.Type.RULES.toString()),


    /**
     * Forces the JCache provider, which can have values client or server, to force the provider type.
     * If not provided, the provider will be client or server, whichever is found on the classpath first respectively.
     */
    JCACHE_PROVIDER_TYPE("hazelcast.jcache.provider.type"),

    /**
     * <p>Enables the Discovery SPI lookup over the old native implementations. This property is temporary and will
     * eventually be removed when the experimental marker is removed.</p>
     * <p>Discovery SPI is <b>disabled</b> by default</p>
     */
    DISCOVERY_SPI_ENABLED("hazelcast.discovery.enabled", false),

    /**
     * <p>Enables the Discovery Joiner to use public ips from DiscoveredNode. This property is temporary and will
     * eventually be removed when the experimental marker is removed.</p>
     * <p>Discovery SPI is <b>disabled</b> by default</p>
     */
    DISCOVERY_SPI_PUBLIC_IP_ENABLED("hazelcast.discovery.public.ip.enabled", false),

    /**
     * Hazelcast serialization version. This is single byte value between 1 and Max supported serialization version.
     *
     * @see BuildInfo#getSerializationVersion()
     */
    SERIALIZATION_VERSION("hazelcast.serialization.version", BuildInfoProvider.getBuildInfo().getSerializationVersion());

    private final String name;
    private final String defaultValue;
    private final TimeUnit timeUnit;
    private final GroupProperty parent;

    GroupProperty(String name) {
        this(name, (String) null);
    }

    GroupProperty(String name, boolean defaultValue) {
        this(name, defaultValue ? "true" : "false");
    }

    GroupProperty(String name, Integer defaultValue) {
        this(name, String.valueOf(defaultValue));
    }

    GroupProperty(String name, Byte defaultValue) {
        this(name, String.valueOf(defaultValue));
    }

    GroupProperty(String name, String defaultValue) {
        this(name, defaultValue, null);
    }

    GroupProperty(String name, Integer defaultValue, TimeUnit timeUnit) {
        this(name, String.valueOf(defaultValue), timeUnit);
    }

    GroupProperty(String name, Long defaultValue, TimeUnit timeUnit) {
        this(name, Long.toString(defaultValue), timeUnit);
    }

    GroupProperty(String name, String defaultValue, TimeUnit timeUnit) {
        this(name, defaultValue, timeUnit, null);
    }

    GroupProperty(String name, GroupProperty groupProperty) {
        this(name, groupProperty.getDefaultValue(), groupProperty.timeUnit, groupProperty);
    }

    GroupProperty(String name, String defaultValue, TimeUnit timeUnit, GroupProperty parent) {
        checkHasText(name, "The property name cannot be null or empty!");

        this.name = name;
        this.defaultValue = defaultValue;
        this.timeUnit = timeUnit;
        this.parent = parent;
    }

    @Override
    public int getIndex() {
        return ordinal();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDefaultValue() {
        return defaultValue;
    }

    @Override
    public TimeUnit getTimeUnit() {
        if (timeUnit == null) {
            throw new IllegalArgumentException(format("groupProperty %s has no TimeUnit defined!", this));
        }
        return timeUnit;
    }

    @Override
    public GroupProperty getParent() {
        return parent;
    }

    @Override
    public void setSystemProperty(String value) {
        System.setProperty(name, value);
    }

    @Override
    public String getSystemProperty() {
        return System.getProperty(name);
    }

    @Override
    public String clearSystemProperty() {
        return System.clearProperty(name);
    }

    @Override
    public String toString() {
        return name;
    }
}
