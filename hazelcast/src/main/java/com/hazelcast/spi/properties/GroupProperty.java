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

package com.hazelcast.spi.properties;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.cluster.fd.ClusterFailureDetectorType;
import com.hazelcast.internal.diagnostics.HealthMonitorLevel;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.query.QueryResultSizeLimiter;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.IndexCopyBehavior;
import com.hazelcast.query.impl.predicates.QueryOptimizerFactory;
import com.hazelcast.spi.InvocationBuilder;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Defines the name and default value for Hazelcast properties.
 */
@SuppressWarnings("checkstyle:javadocvariable")
public final class GroupProperty {

    /**
     * Use this property to verify that Hazelcast nodes only join the cluster when their 'application' level configuration is the
     * same.
     * <p/>
     * If you have multiple machines, you want to make sure that each machine that joins the cluster
     * has exactly the same 'application level' settings (such as settings that are not part of the Hazelcast configuration,
     * maybe some file path). To prevent the machines with potentially different application level configuration from forming
     * a cluster, you can set this property.
     * <p/>
     * You could use actual values, such as string paths, but you can also use an md5 hash. We make a guarantee
     * that nodes will form a cluster (become a member) only if the token is an exact match. If this token is different, the
     * member can't be started and therefore you will get the guarantee that all members in the cluster will have exactly the same
     * application validation token.
     * <p/>
     * This validation token will be checked before a member joins the cluster.
     */
    public static final HazelcastProperty APPLICATION_VALIDATION_TOKEN
            = new HazelcastProperty("hazelcast.application.validation.token");

    /**
     * Total number of partitions in the Hazelcast cluster.
     */
    public static final HazelcastProperty PARTITION_COUNT
            = new HazelcastProperty("hazelcast.partition.count", 271);

    /**
     * The number of partition operation handler threads per member.
     * <p/>
     * If this is less than the number of partitions on a member, partition operations
     * will queue behind other operations of different partitions.
     * <p/>
     * The default is -1, which means that the value is determined dynamically.
     */
    public static final HazelcastProperty PARTITION_OPERATION_THREAD_COUNT
            = new HazelcastProperty("hazelcast.operation.thread.count", -1);

    /**
     * The number of generic operation handler threads per member.
     * <p/>
     * The default is -1, which means that the value is determined dynamically.
     */
    public static final HazelcastProperty GENERIC_OPERATION_THREAD_COUNT
            = new HazelcastProperty("hazelcast.operation.generic.thread.count", -1);

    /**
     * The number of priority generic operation handler threads per member.
     * <p/>
     * The default is 1.
     * <p>
     * Having at least 1 priority generic operation thread helps to improve cluster stability since a lot of cluster
     * operations are generic priority operations and they should get executed as soon as possible. If there is a dedicated
     * generic operation thread then these operations don't get delayed because the generic threads are busy executing regular
     * user operations. So unless memory consumption is an issue, make sure there is at least 1 thread.
     */
    public static final HazelcastProperty PRIORITY_GENERIC_OPERATION_THREAD_COUNT
            = new HazelcastProperty("hazelcast.operation.priority.generic.thread.count", 1);

    /**
     * The number of threads that process responses.
     * <p>
     * By default there are 2 response threads; this gives stable and good performance.
     * <p>
     * If set to 0, the response threads are bypassed and the response handling is done
     * on the IO threads. Under certain conditions this can give a higher throughput, but
     * setting it to 0 should be regarded an experimental feature.
     */
    public static final HazelcastProperty RESPONSE_THREAD_COUNT
            = new HazelcastProperty("hazelcast.operation.response.thread.count", 2);

    /**
     * The number of threads that the client engine has available for processing requests that are not partition specific.
     * Most of the requests, such as {@code map.put} and {@code map.get}, are partition specific and will use a
     * partition-specific operation thread, but there are also requests that can't be executed on a partition-specific operation
     * thread, such as {@code multimap.containsValue(value)}, because they need to access all partitions on a given
     * member.
     *
     * When not set it is set as core-size
     */
    public static final HazelcastProperty CLIENT_ENGINE_THREAD_COUNT
            = new HazelcastProperty("hazelcast.clientengine.thread.count", -1);

    /**
     * The number of threads that the client engine has available for processing requests that are related to transactions
     * When not set it is set as core-size.
     */
    public static final HazelcastProperty CLIENT_ENGINE_QUERY_THREAD_COUNT
            = new HazelcastProperty("hazelcast.clientengine.query.thread.count", -1);

    /**
     * The number of threads that the client engine has available for processing requests that are blocking
     * (example: related to transactions)
     * When not set it is set as core-size * 20.
     */
    public static final HazelcastProperty CLIENT_ENGINE_BLOCKING_THREAD_COUNT
            = new HazelcastProperty("hazelcast.clientengine.blocking.thread.count", -1);

    /**
     * Time after which client connection is removed or owner node of a client is removed from the cluster.
     * <p>
     * ClientDisconnectionOperation runs and cleans all resources of a client (listeners are removed, locks/txn are released).
     * With this property, client has a window to connect back and prevent cleaning up its resources.
     */
    public static final HazelcastProperty CLIENT_ENDPOINT_REMOVE_DELAY_SECONDS
            = new HazelcastProperty("hazelcast.client.endpoint.remove.delay.seconds", 60);

    /**
     * Number of threads for the {@link com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl} executor.
     * The executor is responsible for executing the events. If you process a lot of events and have many cores, setting
     * a higher value is a good practice. This way, more events can be processed in parallel.
     */
    public static final HazelcastProperty EVENT_THREAD_COUNT
            = new HazelcastProperty("hazelcast.event.thread.count", 5);

    /**
     * The capacity of the {@link com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl} executor.
     * The executor is responsible for executing the events. If the events are produced at a higher rate than they are
     * consumed, the queue grows in size. This can lead to an {@link OutOfMemoryError} if the accumulated events
     * are not small enough to fit in memory. This capacity is shared between event topics.
     * When the maximum capacity is reached, the items are dropped. This means that the event system is a 'best effort' system
     * and there is no guarantee that you are going to get an event.
     * Since the capacity is shared between topics, one topic might fill the entire queue and cause
     * other topics to drop their messages.
     */
    public static final HazelcastProperty EVENT_QUEUE_CAPACITY
            = new HazelcastProperty("hazelcast.event.queue.capacity", 1000000);

    /**
     * The timeout for offering an event to the event executor for processing. If the event queue is full,
     * the event might not be accepted to the queue and it will be dropped.
     * This applies only to processing of local events. Remote events (events on a remote subscriber) have no timeout,
     * meaning that the event can be rejected immediately.
     */
    public static final HazelcastProperty EVENT_QUEUE_TIMEOUT_MILLIS
            = new HazelcastProperty("hazelcast.event.queue.timeout.millis", 250, MILLISECONDS);

    /**
     * To prevent overloading of the outbound connections, once in a while an event is made synchronous by wrapping it in a
     * fake operation and waiting for a fake response. This causes the outbound write queue of the connection to get drained.
     * <p>
     * This timeout configures the maximum amount of waiting time for this fake response. Setting it to a too low value
     * can lead to an uncontrolled growth of the outbound write queue of the connection.
     */
    public static final HazelcastProperty EVENT_SYNC_TIMEOUT_MILLIS
            = new HazelcastProperty("hazelcast.event.sync.timeout.millis", 5000, MILLISECONDS);


    public static final HazelcastProperty HEALTH_MONITORING_LEVEL
            = new HazelcastProperty("hazelcast.health.monitoring.level", HealthMonitorLevel.SILENT.toString());
    public static final HazelcastProperty HEALTH_MONITORING_DELAY_SECONDS
            = new HazelcastProperty("hazelcast.health.monitoring.delay.seconds", 20, SECONDS);
    public static final HazelcastProperty HEALTH_MONITORING_THRESHOLD_MEMORY_PERCENTAGE
            = new HazelcastProperty("hazelcast.health.monitoring.threshold.memory.percentage", 70);
    public static final HazelcastProperty HEALTH_MONITORING_THRESHOLD_CPU_PERCENTAGE
            = new HazelcastProperty("hazelcast.health.monitoring.threshold.cpu.percentage", 70);

    /**
     * The number of threads doing socket input and the number of threads doing socket output.
     * <p/>
     * E.g., if 3 is configured, then you get 3 threads doing input and 3 doing output. For individual control,
     * check {@link #IO_INPUT_THREAD_COUNT} and {@link #IO_OUTPUT_THREAD_COUNT}.
     * <p/>
     * The default is 3 (i.e. 6 threads).
     */
    public static final HazelcastProperty IO_THREAD_COUNT
            = new HazelcastProperty("hazelcast.io.thread.count", 3);

    /**
     * Controls the number of socket input threads. By default it is the same as {@link #IO_THREAD_COUNT}.
     */
    public static final HazelcastProperty IO_INPUT_THREAD_COUNT
            = new HazelcastProperty("hazelcast.io.input.thread.count", IO_THREAD_COUNT);

    /**
     * Controls the number of socket output threads. By default it is the same as {@link #IO_THREAD_COUNT}.
     */
    public static final HazelcastProperty IO_OUTPUT_THREAD_COUNT
            = new HazelcastProperty("hazelcast.io.output.thread.count", IO_THREAD_COUNT);

    /**
     * The interval in seconds between {@link com.hazelcast.internal.networking.nio.iobalancer.IOBalancer IOBalancer}
     * executions. The shorter intervals will catch I/O Imbalance faster, but they will cause higher overhead.
     * <p/>
     * Please see the documentation of {@link com.hazelcast.internal.networking.nio.iobalancer.IOBalancer IOBalancer}
     * for a detailed explanation of the problem.
     * <p/>
     * The default is 20 seconds. A value smaller than 1 disables the balancer.
     */
    public static final HazelcastProperty IO_BALANCER_INTERVAL_SECONDS
            = new HazelcastProperty("hazelcast.io.balancer.interval.seconds", 20, SECONDS);

    @SuppressWarnings("checkstyle:constantname")
    public static final HazelcastProperty PREFER_IPv4_STACK
            = new HazelcastProperty("hazelcast.prefer.ipv4.stack", true);

    @Deprecated
    public static final HazelcastProperty VERSION_CHECK_ENABLED
            = new HazelcastProperty("hazelcast.version.check.enabled", true);

    public static final HazelcastProperty PHONE_HOME_ENABLED
            = new HazelcastProperty("hazelcast.phone.home.enabled", true);

    public static final HazelcastProperty CONNECT_ALL_WAIT_SECONDS
            = new HazelcastProperty("hazelcast.connect.all.wait.seconds", 120, SECONDS);

    public static final HazelcastProperty MEMCACHE_ENABLED
            = new HazelcastProperty("hazelcast.memcache.enabled", false);
    public static final HazelcastProperty REST_ENABLED
            = new HazelcastProperty("hazelcast.rest.enabled", false);

    public static final HazelcastProperty HTTP_HEALTHCHECK_ENABLED
            = new HazelcastProperty("hazelcast.http.healthcheck.enabled", false);

    /**
     * The maximum size of the key batch sent to the partition owners for value loading
     * and the maximum size of a key batch for which values are loaded in a single partition.
     */
    public static final HazelcastProperty MAP_LOAD_CHUNK_SIZE
            = new HazelcastProperty("hazelcast.map.load.chunk.size", 1000);

    /**
     * The delay until the first run of the {@link com.hazelcast.internal.cluster.impl.SplitBrainHandler}
     */
    public static final HazelcastProperty MERGE_FIRST_RUN_DELAY_SECONDS
            = new HazelcastProperty("hazelcast.merge.first.run.delay.seconds", 300, SECONDS);

    /**
     * The interval between invocations of the {@link com.hazelcast.internal.cluster.impl.SplitBrainHandler}
     */
    public static final HazelcastProperty MERGE_NEXT_RUN_DELAY_SECONDS
            = new HazelcastProperty("hazelcast.merge.next.run.delay.seconds", 120, SECONDS);

    public static final HazelcastProperty OPERATION_CALL_TIMEOUT_MILLIS
            = new HazelcastProperty("hazelcast.operation.call.timeout.millis", 60000, MILLISECONDS);

    /**
     * If an operation has backups, this property specifies how long the invocation will wait for acks from the backup replicas.
     * If acks are not received from some backups, there will not be any rollback on other successful replicas.
     */
    public static final HazelcastProperty OPERATION_BACKUP_TIMEOUT_MILLIS
            = new HazelcastProperty("hazelcast.operation.backup.timeout.millis", 5000, MILLISECONDS);

    /**
     * When this configuration is enabled, if an operation has sync backups and acks are not received from backup replicas
     * in time, or the member which owns primary replica of the target partition leaves the cluster, then the invocation fails
     * with {@link IndeterminateOperationStateException}. However, even if the invocation fails,
     * there will not be any rollback on other successful replicas.
     */
    public static final HazelcastProperty FAIL_ON_INDETERMINATE_OPERATION_STATE
            = new HazelcastProperty("hazelcast.operation.fail.on.indeterminate.state", false);

    /**
     * Maximum number of retries for an invocation. After threshold is reached, invocation is assumed as failed.
     */
    public static final HazelcastProperty INVOCATION_MAX_RETRY_COUNT
            = new HazelcastProperty("hazelcast.invocation.max.retry.count", InvocationBuilder.DEFAULT_TRY_COUNT);

    /**
     * Pause time between each retry cycle of an invocation in milliseconds.
     */
    public static final HazelcastProperty INVOCATION_RETRY_PAUSE
            = new HazelcastProperty("hazelcast.invocation.retry.pause.millis",
            InvocationBuilder.DEFAULT_TRY_PAUSE_MILLIS, MILLISECONDS);

    public static final HazelcastProperty SOCKET_BIND_ANY
            = new HazelcastProperty("hazelcast.socket.bind.any", true);
    public static final HazelcastProperty SOCKET_SERVER_BIND_ANY
            = new HazelcastProperty("hazelcast.socket.server.bind.any", SOCKET_BIND_ANY);
    public static final HazelcastProperty SOCKET_CLIENT_BIND_ANY
            = new HazelcastProperty("hazelcast.socket.client.bind.any", SOCKET_BIND_ANY);
    public static final HazelcastProperty SOCKET_CLIENT_BIND
            = new HazelcastProperty("hazelcast.socket.client.bind", true);

    // number of kilobytes
    public static final HazelcastProperty SOCKET_RECEIVE_BUFFER_SIZE
            = new HazelcastProperty("hazelcast.socket.receive.buffer.size", 128);

    // number of kilobytes
    public static final HazelcastProperty SOCKET_SEND_BUFFER_SIZE
            = new HazelcastProperty("hazelcast.socket.send.buffer.size", 128);

    /**
     * If the bytebuffers used in the socket should be a direct bytebuffer (true) or a regular bytebuffer (false).
     */
    public static final HazelcastProperty SOCKET_BUFFER_DIRECT
            = new HazelcastProperty("hazelcast.socket.buffer.direct", false);


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
    public static final HazelcastProperty SOCKET_CLIENT_RECEIVE_BUFFER_SIZE
            = new HazelcastProperty("hazelcast.socket.client.receive.buffer.size", -1);

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
    public static final HazelcastProperty SOCKET_CLIENT_SEND_BUFFER_SIZE
            = new HazelcastProperty("hazelcast.socket.client.send.buffer.size", -1);

    public static final HazelcastProperty SOCKET_CLIENT_BUFFER_DIRECT
            = new HazelcastProperty("hazelcast.socket.client.buffer.direct", false);

    public static final HazelcastProperty SOCKET_LINGER_SECONDS
            = new HazelcastProperty("hazelcast.socket.linger.seconds", 0, SECONDS);
    public static final HazelcastProperty SOCKET_CONNECT_TIMEOUT_SECONDS
            = new HazelcastProperty("hazelcast.socket.connect.timeout.seconds", 0, SECONDS);
    public static final HazelcastProperty SOCKET_KEEP_ALIVE
            = new HazelcastProperty("hazelcast.socket.keep.alive", true);
    public static final HazelcastProperty SOCKET_NO_DELAY
            = new HazelcastProperty("hazelcast.socket.no.delay", true);

    public static final HazelcastProperty SHUTDOWNHOOK_ENABLED
            = new HazelcastProperty("hazelcast.shutdownhook.enabled", true);

    /**
     * Behaviour when JVM is about to exit while Hazelcast instance is still running.
     *
     * Possible values:
     * TERMINATE: Terminate Hazelcast immediately
     * GRACEFUL:  Initiate graceful shutdown. This can significantly slow-down JVM exit process, but it's tries to
     * retain data safety.
     *
     * Default: TERMINATE
     *
     * You should always shutdown Hazelcast explicitly via {@link HazelcastInstance#shutdown()}
     * It's not recommended to rely on shutdown hook, this is a last-effort measure.
     */
    public static final HazelcastProperty SHUTDOWNHOOK_POLICY
            = new HazelcastProperty("hazelcast.shutdownhook.policy", "TERMINATE");

    public static final HazelcastProperty WAIT_SECONDS_BEFORE_JOIN
            = new HazelcastProperty("hazelcast.wait.seconds.before.join", 5, SECONDS);

    public static final HazelcastProperty MAX_WAIT_SECONDS_BEFORE_JOIN
            = new HazelcastProperty("hazelcast.max.wait.seconds.before.join", 20, SECONDS);
    public static final HazelcastProperty MAX_JOIN_SECONDS
            = new HazelcastProperty("hazelcast.max.join.seconds", 300, SECONDS);
    public static final HazelcastProperty MAX_JOIN_MERGE_TARGET_SECONDS
            = new HazelcastProperty("hazelcast.max.join.merge.target.seconds", 20, SECONDS);

    /**
     * The interval at which member heartbeat messages are sent
     */
    public static final HazelcastProperty HEARTBEAT_INTERVAL_SECONDS
            = new HazelcastProperty("hazelcast.heartbeat.interval.seconds", 5, SECONDS);

    /**
     * The timeout which defines when master candidate gives up waiting for response to its mastership claim.
     * After timeout happens, non-responding member will be removed from member list.
     */
    public static final HazelcastProperty MASTERSHIP_CLAIM_TIMEOUT_SECONDS
            = new HazelcastProperty("hazelcast.mastership.claim.timeout.seconds", 120, SECONDS);
    /**
     * The timeout which defines when a cluster member is removed because it has not sent any heartbeats.
     */
    public static final HazelcastProperty MAX_NO_HEARTBEAT_SECONDS
            = new HazelcastProperty("hazelcast.max.no.heartbeat.seconds", 60, SECONDS);

    /**
     * The interval at which master confirmations are sent from non-master nodes to the master node
     *
     * @deprecated since 3.10
     */
    @Deprecated
    public static final HazelcastProperty MASTER_CONFIRMATION_INTERVAL_SECONDS
            = new HazelcastProperty("hazelcast.master.confirmation.interval.seconds", 30, SECONDS);
    /**
     * The timeout which defines when a cluster member is removed because it has not sent any master confirmations.
     *
     * @deprecated since 3.10
     */
    @Deprecated
    public static final HazelcastProperty MAX_NO_MASTER_CONFIRMATION_SECONDS
            = new HazelcastProperty("hazelcast.max.no.master.confirmation.seconds", 150, SECONDS);

    /**
     * Heartbeat failure detector type. Available options are:
     * <ul>
     * <li><code>deadline</code>:  A deadline based failure detector uses an absolute timeout
     * for missing/lost heartbeats. After timeout member is considered as dead/unavailable.
     * </li>
     * <li><code>phi-accrual</code>: Implementation of 'The Phi Accrual Failure Detector' by Hayashibara et al.
     * as defined in their paper. Phi Accrual Failure Detector is adaptive to network/environment conditions,
     * that's why a lower {@link #MAX_NO_HEARTBEAT_SECONDS} (for example 10 or 15 seconds) can be used to provide
     * faster detection of unavailable members.
     * </li>
     * </ul>
     *
     * Default failure detector is <code>deadline</code>.
     */
    public static final HazelcastProperty HEARTBEAT_FAILURE_DETECTOR_TYPE
            = new HazelcastProperty("hazelcast.heartbeat.failuredetector.type", ClusterFailureDetectorType.DEADLINE.toString());

    /**
     * The interval at which the master sends the member lists are sent to other non-master members
     */
    public static final HazelcastProperty MEMBER_LIST_PUBLISH_INTERVAL_SECONDS
            = new HazelcastProperty("hazelcast.member.list.publish.interval.seconds", 60, SECONDS);

    public static final HazelcastProperty CLIENT_HEARTBEAT_TIMEOUT_SECONDS
            = new HazelcastProperty("hazelcast.client.max.no.heartbeat.seconds", 300, SECONDS);

    public static final HazelcastProperty CLUSTER_SHUTDOWN_TIMEOUT_SECONDS
            = new HazelcastProperty("hazelcast.cluster.shutdown.timeout.seconds", 900, SECONDS);

    /**
     * If a member should be pinged when a sufficient amount of heartbeats have passed and the member has not sent any
     * heartbeats. If the member is not reachable, it will be removed.
     *
     * @deprecated as of 3.10 this can be configured through {@link com.hazelcast.config.IcmpFailureDetectorConfig}
     * This will be removed in future versions. Until this is done,
     * if the {@link com.hazelcast.config.IcmpFailureDetectorConfig} is null we will still fall back to this
     */
    public static final HazelcastProperty ICMP_ENABLED
            = new HazelcastProperty("hazelcast.icmp.enabled", false);

    /**
     * Run ICMP detection in parallel with the Heartbeat failure detector.
     *
     * @deprecated as of 3.10 this can be configured through {@link com.hazelcast.config.IcmpFailureDetectorConfig}
     * This will be removed in future versions. Until this is done,
     * if the {@link com.hazelcast.config.IcmpFailureDetectorConfig} is null we will still fall back to this
     */
    public static final HazelcastProperty ICMP_PARALLEL_MODE
            = new HazelcastProperty("hazelcast.icmp.parallel.mode", true);

    /**
     * Enforce ICMP Echo Request mode for ping-detector. If OS is not supported,
     * or not configured correctly as per reference-manual, hazelcast will fail to start.
     *
     * @deprecated as of 3.10 this can be configured through {@link com.hazelcast.config.IcmpFailureDetectorConfig}
     * This will be removed in future versions. Until this is done,
     * if the {@link com.hazelcast.config.IcmpFailureDetectorConfig} is null we will still fall back to this
     */
    public static final HazelcastProperty ICMP_ECHO_FAIL_FAST
            = new HazelcastProperty("hazelcast.icmp.echo.fail.fast.on.startup", true);

    /**
     * Ping timeout in milliseconds. This cannot be more than the interval value. Should always be smaller.
     *
     * @deprecated as of 3.10 this can be configured through {@link com.hazelcast.config.IcmpFailureDetectorConfig}
     * This will be removed in future versions. Until this is done,
     * if the {@link com.hazelcast.config.IcmpFailureDetectorConfig} is null we will still fall back to this
     */
    public static final HazelcastProperty ICMP_TIMEOUT
            = new HazelcastProperty("hazelcast.icmp.timeout", 1000, MILLISECONDS);

    /**
     * Interval between ping attempts in milliseconds. Default and min allowed, 1 second.
     *
     * @deprecated as of 3.10 this can be configured through {@link com.hazelcast.config.IcmpFailureDetectorConfig}
     * This will be removed in future versions. Until this is done,
     * if the {@link com.hazelcast.config.IcmpFailureDetectorConfig} is null we will still fall back to this
     */
    public static final HazelcastProperty ICMP_INTERVAL
            = new HazelcastProperty("hazelcast.icmp.interval", 1000, MILLISECONDS);

    /**
     * Max ping attempts before suspecting a member
     *
     * @deprecated as of 3.10 this can be configured through {@link com.hazelcast.config.IcmpFailureDetectorConfig}
     * This will be removed in future versions. Until this is done,
     * if the {@link com.hazelcast.config.IcmpFailureDetectorConfig} is null we will still fall back to this
     */
    public static final HazelcastProperty ICMP_MAX_ATTEMPTS
            = new HazelcastProperty("hazelcast.icmp.max.attempts", 3);

    /**
     * Ping TTL, the maximum number of hops the packets should go through or 0 for the default.
     * Zero in this case means unlimited hops.
     *
     * @deprecated as of 3.10 this can be configured through {@link com.hazelcast.config.IcmpFailureDetectorConfig}
     * This will be removed in future versions. Until this is done,
     * if the {@link com.hazelcast.config.IcmpFailureDetectorConfig} is null we will still fall back to this
     */
    public static final HazelcastProperty ICMP_TTL
            = new HazelcastProperty("hazelcast.icmp.ttl", 0);

    public static final HazelcastProperty INITIAL_MIN_CLUSTER_SIZE
            = new HazelcastProperty("hazelcast.initial.min.cluster.size", 0);
    public static final HazelcastProperty INITIAL_WAIT_SECONDS
            = new HazelcastProperty("hazelcast.initial.wait.seconds", 0, SECONDS);

    /**
     * The number of incremental ports, starting with port number defined in network configuration,
     * that will be used to connect to a host which is defined without a port in the TCP-IP member list
     * while a node is searching for a cluster.
     */
    public static final HazelcastProperty TCP_JOIN_PORT_TRY_COUNT
            = new HazelcastProperty("hazelcast.tcp.join.port.try.count", 3);

    public static final HazelcastProperty MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS
            = new HazelcastProperty("hazelcast.map.replica.scheduled.task.delay.seconds", 10, SECONDS);

    /**
     * You can use MAP_EXPIRY_DELAY_SECONDS to deal with some possible
     * edge cases, such as using EntryProcessor. Without this delay, you
     * may see that an EntryProcessor running on the owner partition
     * found a key, but EntryBackupProcessor did not find it on backup,
     * and as a result when backup promotes to owner you will end up
     * with an unprocessed key.
     */
    public static final HazelcastProperty MAP_EXPIRY_DELAY_SECONDS
            = new HazelcastProperty("hazelcast.map.expiry.delay.seconds", 10, SECONDS);

    /**
     * Maximum number of IMap entries Hazelcast will evict during a
     * single eviction cycle. Eviction cycle is triggered by a map
     * mutation. Typically it's OK to evict at most a single entry.
     * However imagine the scenario where you are inserting values in a
     * loop and in each iteration you double entry size. In this
     * situation Hazelcast has to evict more than just a single entry -
     * as all existing entries are smaller than the entry which is about
     * to be added and removing any old entry cannot make sufficient
     * room for the new entry.
     *
     * Default: 1
     */
    public static final HazelcastProperty MAP_EVICTION_BATCH_SIZE
            = new HazelcastProperty("hazelcast.map.eviction.batch.size", 1);

    /**
     * This property is a switch between old and new event publishing
     * behavior of map#loadAll. When it is true, map#loadAll publishes
     * entry ADDED events, when false, map#loadAll publishes entry
     * LOADED events. By default LOADED events will be published.
     *
     * @since 3.11
     */
    public static final HazelcastProperty MAP_LOAD_ALL_PUBLISHES_ADDED_EVENT
            = new HazelcastProperty("hazelcast.map.loadAll.publishes.added.event", false);

    public static final HazelcastProperty LOGGING_TYPE
            = new HazelcastProperty("hazelcast.logging.type", "jdk");

    public static final HazelcastProperty ENABLE_JMX
            = new HazelcastProperty("hazelcast.jmx", false);
    public static final HazelcastProperty JMX_UPDATE_INTERVAL_SECONDS
            = new HazelcastProperty("hazelcast.jmx.update.interval.seconds", 5, SECONDS);

    public static final HazelcastProperty MC_MAX_VISIBLE_SLOW_OPERATION_COUNT
            = new HazelcastProperty("hazelcast.mc.max.visible.slow.operations.count", 10);
    public static final HazelcastProperty MC_URL_CHANGE_ENABLED
            = new HazelcastProperty("hazelcast.mc.url.change.enabled", true);

    public static final HazelcastProperty CONNECTION_MONITOR_INTERVAL
            = new HazelcastProperty("hazelcast.connection.monitor.interval", 100, MILLISECONDS);
    public static final HazelcastProperty CONNECTION_MONITOR_MAX_FAULTS
            = new HazelcastProperty("hazelcast.connection.monitor.max.faults", 3);
    /**
     * Time in seconds to sleep after a migration task.
     */
    public static final HazelcastProperty PARTITION_MIGRATION_INTERVAL
            = new HazelcastProperty("hazelcast.partition.migration.interval", 0, SECONDS);
    /**
     * Timeout in seconds for all migration operations.
     */
    public static final HazelcastProperty PARTITION_MIGRATION_TIMEOUT
            = new HazelcastProperty("hazelcast.partition.migration.timeout", 300, SECONDS);
    public static final HazelcastProperty PARTITION_FRAGMENTED_MIGRATION_ENABLED
            = new HazelcastProperty("hazelcast.partition.migration.fragments.enabled", true);
    public static final HazelcastProperty DISABLE_STALE_READ_ON_PARTITION_MIGRATION
            = new HazelcastProperty("hazelcast.partition.migration.stale.read.disabled", false);

    public static final HazelcastProperty PARTITION_TABLE_SEND_INTERVAL
            = new HazelcastProperty("hazelcast.partition.table.send.interval", 15, SECONDS);
    public static final HazelcastProperty PARTITION_BACKUP_SYNC_INTERVAL
            = new HazelcastProperty("hazelcast.partition.backup.sync.interval", 30, SECONDS);
    public static final HazelcastProperty PARTITION_MAX_PARALLEL_REPLICATIONS
            = new HazelcastProperty("hazelcast.partition.max.parallel.replications", 5);
    public static final HazelcastProperty PARTITIONING_STRATEGY_CLASS
            = new HazelcastProperty("hazelcast.partitioning.strategy.class", "");

    public static final HazelcastProperty GRACEFUL_SHUTDOWN_MAX_WAIT
            = new HazelcastProperty("hazelcast.graceful.shutdown.max.wait", 600, SECONDS);

    /**
     * Enables or disables the {@link com.hazelcast.spi.impl.operationexecutor.slowoperationdetector.SlowOperationDetector}.
     */
    public static final HazelcastProperty SLOW_OPERATION_DETECTOR_ENABLED
            = new HazelcastProperty("hazelcast.slow.operation.detector.enabled", true);

    /**
     * Defines a threshold above which a running operation in {@link com.hazelcast.spi.OperationService} is considered to be slow.
     * These operations will log a warning and will be shown in the Management Center with detailed information, e.g. stacktrace.
     */
    public static final HazelcastProperty SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS
            = new HazelcastProperty("hazelcast.slow.operation.detector.threshold.millis", 10000, MILLISECONDS);

    /**
     * This value defines the retention time of invocations in slow operation logs.
     * <p/>
     * If an invocation is older than this value, it will be purged from the log to prevent unlimited memory usage.
     * When all invocations are purged from a log, the log itself will be deleted.
     *
     * @see #SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS
     */
    public static final HazelcastProperty SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS
            = new HazelcastProperty("hazelcast.slow.operation.detector.log.retention.seconds", 3600, SECONDS);

    /**
     * Purge interval for slow operation logs.
     *
     * @see #SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS
     */
    public static final HazelcastProperty SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS
            = new HazelcastProperty("hazelcast.slow.operation.detector.log.purge.interval.seconds", 300, SECONDS);

    /**
     * Defines if the stacktraces of slow operations are logged in the log file. Stacktraces will always be reported to the
     * Management Center, but by default they are not printed to keep the log size small.
     */
    public static final HazelcastProperty SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED
            = new HazelcastProperty("hazelcast.slow.operation.detector.stacktrace.logging.enabled", false);

    /**
     * Property isn't used anymore.
     */
    @Deprecated
    public static final HazelcastProperty SLOW_INVOCATION_DETECTOR_THRESHOLD_MILLIS
            = new HazelcastProperty("hazelcast.slow.invocation.detector.threshold.millis", -1, MILLISECONDS);

    public static final HazelcastProperty LOCK_MAX_LEASE_TIME_SECONDS
            = new HazelcastProperty("hazelcast.lock.max.lease.time.seconds", Long.MAX_VALUE, SECONDS);

    public static final HazelcastProperty ENTERPRISE_LICENSE_KEY
            = new HazelcastProperty("hazelcast.enterprise.license.key");

    /**
     * Setting this capacity is valid if you set {@link com.hazelcast.config.MapStoreConfig#writeCoalescing} to {@code false}.
     * Otherwise its value will not be taken into account.
     * <p/>
     * The per node maximum write-behind queue capacity is the total of all write-behind queue sizes in a node, including backups.
     * <p/>
     * The maximum value which can be set is {@link Integer#MAX_VALUE}
     */
    public static final HazelcastProperty MAP_WRITE_BEHIND_QUEUE_CAPACITY
            = new HazelcastProperty("hazelcast.map.write.behind.queue.capacity", 50000);

    /**
     * Defines cache invalidation event batch sending is enabled or not.
     */
    public static final HazelcastProperty CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED
            = new HazelcastProperty("hazelcast.cache.invalidation.batch.enabled", true);

    /**
     * Defines the maximum number of cache invalidation events to be drained and sent to the event listeners in a batch.
     */
    public static final HazelcastProperty CACHE_INVALIDATION_MESSAGE_BATCH_SIZE
            = new HazelcastProperty("hazelcast.cache.invalidation.batch.size", 100);

    /**
     * Defines the cache invalidation event batch sending frequency in seconds.
     * <p/>
     * When the number of events do not come up to {@link #CACHE_INVALIDATION_MESSAGE_BATCH_SIZE} in the given time period (which
     * is defined by this property); those events are gathered into a batch and sent to target.
     */
    public static final HazelcastProperty CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS
            = new HazelcastProperty("hazelcast.cache.invalidation.batchfrequency.seconds", 10, SECONDS);

    /**
     * Defines Near Cache invalidation event batch sending is enabled or not.
     */
    public static final HazelcastProperty MAP_INVALIDATION_MESSAGE_BATCH_ENABLED
            = new HazelcastProperty("hazelcast.map.invalidation.batch.enabled", true);

    /**
     * Defines the maximum number of Near Cache invalidation events to be drained and sent to the event Near Cache in a batch.
     */
    public static final HazelcastProperty MAP_INVALIDATION_MESSAGE_BATCH_SIZE
            = new HazelcastProperty("hazelcast.map.invalidation.batch.size", 100);

    /**
     * Defines the Near Cache invalidation event batch sending frequency in seconds.
     * <p/>
     * When the number of events do not come up to {@link #MAP_INVALIDATION_MESSAGE_BATCH_SIZE} in the given time period (which
     * is defined by this property); those events are gathered into a batch and sent to target.
     */
    public static final HazelcastProperty MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS
            = new HazelcastProperty("hazelcast.map.invalidation.batchfrequency.seconds", 10, SECONDS);

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
     *
     * To deal with overloads of backups, the property 'hazelcast.operation.backup.timeout.millis' should be set to a larger
     * value; above 60000 is recommended. Otherwise it can still happen backups accumulate.
     */
    public static final HazelcastProperty BACKPRESSURE_ENABLED
            = new HazelcastProperty("hazelcast.backpressure.enabled", false);

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
    public static final HazelcastProperty BACKPRESSURE_SYNCWINDOW
            = new HazelcastProperty("hazelcast.backpressure.syncwindow", 100);

    /**
     * Control the maximum timeout in millis to wait for an invocation space to be available.
     * <p/>
     * If an invocation can't be made because there are too many pending invocations, then an exponential backoff is done
     * to give the system time to deal with the backlog of invocations. This property controls how long an invocation is
     * allowed to wait before getting a {@link com.hazelcast.core.HazelcastOverloadException}.
     * <p/>
     * The value needs to be equal or larger than 0.
     */
    public static final HazelcastProperty BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS
            = new HazelcastProperty("hazelcast.backpressure.backoff.timeout.millis", 60000, MILLISECONDS);

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
    public static final HazelcastProperty BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION
            = new HazelcastProperty("hazelcast.backpressure.max.concurrent.invocations.per.partition", 100);

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
    public static final HazelcastProperty QUERY_PREDICATE_PARALLEL_EVALUATION
            = new HazelcastProperty("hazelcast.query.predicate.parallel.evaluation", false);

    /**
     * Run aggregation accumulation for multiple entries in parallel.
     * <p/>
     * Each Hazelcast member executes the accumulation stage of an aggregation using a single thread by default.
     * In most cases it pays off to do it in parallel.
     * <p/>
     * The default is true.
     */
    public static final HazelcastProperty AGGREGATION_ACCUMULATION_PARALLEL_EVALUATION
            = new HazelcastProperty("hazelcast.aggregation.accumulation.parallel.evaluation", true);

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
    public static final HazelcastProperty QUERY_RESULT_SIZE_LIMIT
            = new HazelcastProperty("hazelcast.query.result.size.limit", -1);

    /**
     * Maximum value of local partitions to trigger local pre-check for {@link TruePredicate} query operations on maps.
     * <p/>
     * To limit the result size of a query ({@see PROP_QUERY_RESULT_SIZE_LIMIT}); a local pre-check on the requesting node can be
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
    public static final HazelcastProperty QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK
            = new HazelcastProperty("hazelcast.query.max.local.partition.limit.for.precheck", 3);

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
    public static final HazelcastProperty QUERY_OPTIMIZER_TYPE
            = new HazelcastProperty("hazelcast.query.optimizer.type", QueryOptimizerFactory.Type.RULES.toString());

    /**
     * Type of Query Index result copying behavior.
     *
     * Defines the behavior for index copying on index read/write.
     *
     * Supported in BINARY and OBJECT in-memory-formats. Ignored in NATIVE in-memory-format.
     *
     * Why is it needed? In order to support correctness the internal data-structures used by indexes need to do some copying.
     * The copying may take place on-read or on-write:
     *
     * -> Copying on-read means that each index-read operation will copy the result of the query before returning it to the
     * caller.This copying may be expensive, depending on the size of the result, since the result is stored in a map, which
     * means that all entries need to have the hash calculated before being stored in a bucket.
     * Each index-write operation however will be fast, since there will be no copying taking place.
     *
     * -> Copying on-write means that each index-write operation will completely copy the underlying map to provide the
     * copy-on-write semantics. Depending on the index size, it may be a very expensive operation.
     * Each index-read operation will be very fast, however, since it may just access the map and return it to the caller.
     *
     * -> Never copying is tricky. It means that the internal data structures of the index are concurrently modified without
     * copy-on-write semantics. Index reads never copy the results of a query to a separate map.
     * It means that the results backed by the underlying index-map can change after the query has been executed.
     * Specifically an entry might have been added / removed from an index, or it might have been remapped.
     * Should be used in cases when a the caller expects "mostly correct" results - specifically, if it's ok
     * if some entries returned in the result set do not match the initial query criteria.
     * The fastest solution for read and writes, since no copying takes place.
     *
     * It's a tuneable trade-off - the user may decide.
     *
     * Valid Values:
     * <ul>
     * <li>COPY_ON_READY - Internal data structures of the index are concurrently modified without copy-on-write semantics.
     * Index queries copy the results of a query on index read to detach the result from the source map.
     * Should be used in index-write intensive cases, since the reads will slow down due to the copying.
     * Default value.
     * </li>
     * <li>COPY_ON_WRITE - Internal data structures of the index are modified with copy-on-write semantics.
     * Previously returned index query results reflect the state of the index at the time of the query and are not
     * affected by future index modifications.
     * Should be used in index-read intensive cases, since the writes will slow down due to the copying.
     * </li>
     * <li>NEVER - Internal data structures of the index are concurrently modified without copy-on-write semantics.
     * Index reads never copy the results of a query to a separate map.
     * It means that the results backed by the underlying index-map can change after the query has been executed.
     * Specifically an entry might have been added / removed from an index, or it might have been remapped.
     * Should be used in cases when a the caller expects "mostly correct" results - specifically, if it's ok
     * if some entries returned in the result set do not match the initial query criteria.
     * The fastest solution for read and writes, since no copying takes place.</li>
     * </ul>
     * <p/>
     */
    public static final HazelcastProperty INDEX_COPY_BEHAVIOR
            = new HazelcastProperty("hazelcast.index.copy.behavior", IndexCopyBehavior.COPY_ON_READ.toString());

    /**
     * Forces the JCache provider, which can have values client or server, to force the provider type.
     * If not provided, the provider will be client or server, whichever is found on the classpath first respectively.
     */
    public static final HazelcastProperty JCACHE_PROVIDER_TYPE
            = new HazelcastProperty("hazelcast.jcache.provider.type");

    /**
     * <p>Enables the Discovery SPI lookup over the old native implementations. This property is temporary and will
     * eventually be removed when the experimental marker is removed.</p>
     * <p>Discovery SPI is <b>disabled</b> by default</p>
     */
    public static final HazelcastProperty DISCOVERY_SPI_ENABLED
            = new HazelcastProperty("hazelcast.discovery.enabled", false);

    /**
     * <p>Enables the Discovery Joiner to use public ips from DiscoveredNode. This property is temporary and will
     * eventually be removed when the experimental marker is removed.</p>
     * <p>Discovery SPI is <b>disabled</b> by default</p>
     */
    public static final HazelcastProperty DISCOVERY_SPI_PUBLIC_IP_ENABLED
            = new HazelcastProperty("hazelcast.discovery.public.ip.enabled", false);

    /**
     * When this property is true, if the server can not determine the connected client version, it shall assume that it is of
     * 3.6.x version client. This property is especially needed if you are using ICache (or JCache).
     */
    public static final HazelcastProperty COMPATIBILITY_3_6_CLIENT_ENABLED
            = new HazelcastProperty("hazelcast.compatibility.3.6.client", false);

    /**
     * Hazelcast serialization version. This is single byte value between 1 and Max supported serialization version.
     *
     * @see BuildInfo#getSerializationVersion()
     */
    public static final HazelcastProperty SERIALIZATION_VERSION
            = new HazelcastProperty("hazelcast.serialization.version",
            BuildInfoProvider.getBuildInfo().getSerializationVersion());

    /**
     * Override cluster version to use while node is not yet member of a cluster. The cluster version assumed before joining
     * a cluster may affect the serialization format of cluster discovery & join operations and its compatibility with members
     * of a cluster running on different Hazelcast codebase versions. The default is to use the node's codebase version. You may
     * need to override it for your node to join a cluster running on a previous cluster version.
     */
    public static final HazelcastProperty INIT_CLUSTER_VERSION
            = new HazelcastProperty("hazelcast.init.cluster.version");

    /**
     * Enables legacy (pre-3.9) member list format which is printed in logs. New format is introduced by 3.9
     * includes member list version.
     */
    public static final HazelcastProperty USE_LEGACY_MEMBER_LIST_FORMAT
            = new HazelcastProperty("hazelcast.legacy.memberlist.format.enabled", false);

    /**
     * Controls whether we apply more strict checks upon BIND requests towards a cluster member.
     * The checks mainly validate the remote BIND request against the remote address as found in the socket.
     * By default they are disabled, to avoid connectivity issues when deployed under NAT'ed infrastructure.
     */
    public static final HazelcastProperty BIND_SPOOFING_CHECKS =
            new HazelcastProperty("hazelcast.nio.tcp.spoofing.checks", false);

    /**
     * This is a Java 6 specific property. In Java 7+ tasks are always removed
     * on cancellation due to the explicit
     * {@code java.util.concurrent.ScheduledThreadPoolExecutor#setRemoveOnCancelPolicy(boolean)}
     * and constant time removal.
     *
     * In Java 6 there is no out-of-the-box support for removal of cancelled tasks,
     * and the only way to implement this is using a linear scan of all pending
     * tasks. Therefore in Java 6 there is a performance penalty.
     *
     * Using this property, in Java 6, one can control if cancelled tasks are removed.
     * By default tasks are removed, because it can lead to temporary retention
     * of memory if there a large volume of pending cancelled tasks. And this can
     * lead to gc/performance problems as we saw with the transaction tests.
     *
     * However if this automatic removal of cancelled tasks start to become a
     * performance problem, it can be disabled in Java 6.
     *
     * For more information see the {@link com.hazelcast.util.executor.LoggingScheduledExecutor}.
     */
    public static final HazelcastProperty TASK_SCHEDULER_REMOVE_ON_CANCEL =
            new HazelcastProperty("hazelcast.executionservice.taskscheduler.remove.oncancel", true);

    /**
     * By default, search for data structures config is performed within static configuration first:
     * <ul>
     * <li>Exact match in static config</li>
     * <li>Wildcard match in static config</li>
     * <li>Exact match in dynamic config</li>
     * <li>Wildcard match in dynamic config</li>
     * <li>Fallback to default</li>
     * </ul>
     * But sometimes it makes sense to perform search within dynamic configs first. If this property is set to
     * <code>true</code>, search algorithm changes to:
     * <ul>
     * <li>Exact match in dynamic config</li>
     * <li>Wildcard match in dynamic config</li>
     * <li>Exact match in static config</li>
     * <li>Wildcard match in static config</li>
     * <li>Fallback to default</li>
     * </ul>
     */
    public static final HazelcastProperty SEARCH_DYNAMIC_CONFIG_FIRST
            = new HazelcastProperty("hazelcast.data.search.dynamic.config.first.enabled", false);

    private GroupProperty() {
    }
}
