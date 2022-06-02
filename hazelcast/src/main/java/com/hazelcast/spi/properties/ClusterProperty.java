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

package com.hazelcast.spi.properties;

import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.MetricsConfig;
import com.hazelcast.config.MetricsJmxConfig;
import com.hazelcast.config.MetricsManagementCenterConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.cluster.fd.ClusterFailureDetectorType;
import com.hazelcast.internal.diagnostics.HealthMonitorLevel;
import com.hazelcast.internal.util.OsHelper;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.query.QueryResultSizeLimiter;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.IndexCopyBehavior;
import com.hazelcast.query.impl.predicates.QueryOptimizerFactory;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.Map;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Defines the name and default value for Hazelcast properties.
 */
@SuppressWarnings({"checkstyle:magicnumber"})
public final class ClusterProperty {
    /*
     * NETWORKING / TCP PROPERTIES
     */
    /**
     * The interval at which network stats (bytes sent and received) are
     * re-calculated and published.
     * Used only when Advanced Networking is enabled.
     */
    public static final HazelcastProperty NETWORK_STATS_REFRESH_INTERVAL_SECONDS
            = new HazelcastProperty("hazelcast.network.stats.refresh.interval.seconds", 3, SECONDS);

    /**
     * Number of parallel connection between members.
     * <p>
     * There is no point in having more connections than IO threads.
     */
    public static final HazelcastProperty CHANNEL_COUNT
            = new HazelcastProperty("tcp.channels.per.connection", 1);

    /**
     * The interval in seconds between
     * {@link com.hazelcast.internal.networking.nio.iobalancer.IOBalancer}
     * executions. The shorter intervals will catch I/O Imbalance faster, but
     * they will cause higher overhead.
     * <p>
     * Please see the documentation of
     * {@link com.hazelcast.internal.networking.nio.iobalancer.IOBalancer}
     * for a detailed explanation of the problem.
     * <p>
     * The default is 20 seconds. A value smaller than 1 disables the balancer.
     */
    public static final HazelcastProperty IO_BALANCER_INTERVAL_SECONDS
            = new HazelcastProperty("hazelcast.io.balancer.interval.seconds", 20, SECONDS);

    /**
     * Prefer IPv4 network interface when picking a local address.
     */
    @SuppressWarnings("checkstyle:constantname")
    public static final HazelcastProperty PREFER_IPv4_STACK
            = new HazelcastProperty("hazelcast.prefer.ipv4.stack", true);

    /**
     * Bind both server-socket and client-sockets to any local interface.
     */
    public static final HazelcastProperty SOCKET_BIND_ANY
            = new HazelcastProperty("hazelcast.socket.bind.any", true);

    /**
     * Bind server-socket to any local interface. If not set,
     * {@link #SOCKET_BIND_ANY} is used as the default.
     */
    public static final HazelcastProperty SOCKET_SERVER_BIND_ANY
            = new HazelcastProperty("hazelcast.socket.server.bind.any", SOCKET_BIND_ANY);

    /**
     * Bind client-sockets to any local interface. If not set,
     * {@link #SOCKET_BIND_ANY} is used as the default.
     */
    public static final HazelcastProperty SOCKET_CLIENT_BIND_ANY
            = new HazelcastProperty("hazelcast.socket.client.bind.any", SOCKET_BIND_ANY);

    /**
     * Bind client socket to an interface when connecting to a remote server
     * socket. When set to false, client socket is not bound to any interface.
     */
    public static final HazelcastProperty SOCKET_CLIENT_BIND
            = new HazelcastProperty("hazelcast.socket.client.bind", true);

    /**
     * Socket receive buffer (SO_RCVBUF) size in KB. If you have a very fast
     * network, e.g., 10gbit) and/or you have large entries, then you may benefit
     * from increasing sender/receiver buffer sizes. Use this property and the
     * next one below tune the size.
     */
    public static final HazelcastProperty SOCKET_RECEIVE_BUFFER_SIZE
            = new HazelcastProperty("hazelcast.socket.receive.buffer.size", 128);

    /**
     * Socket send buffer (SO_SNDBUF) size in KB.
     */
    public static final HazelcastProperty SOCKET_SEND_BUFFER_SIZE
            = new HazelcastProperty("hazelcast.socket.send.buffer.size", 128);

    /**
     * If the bytebuffers used in the socket should be a direct bytebuffer ({@code true})
     * or a regular bytebuffer ({@code false}).
     */
    public static final HazelcastProperty SOCKET_BUFFER_DIRECT
            = new HazelcastProperty("hazelcast.socket.buffer.direct", false);

    /**
     * Overrides receive buffer size for connections opened by clients.
     * <p>
     * Hazelcast creates all connections with receive buffer size set according
     * to {@link #SOCKET_RECEIVE_BUFFER_SIZE}.
     * When it detects a connection was opened by a client then it adjusts receive
     * buffer size according to this property.
     * <p>
     * Size is in kilobytes.
     * <p>
     * The default is -1 (same as receive buffer size for connections opened by members).
     */
    public static final HazelcastProperty SOCKET_CLIENT_RECEIVE_BUFFER_SIZE
            = new HazelcastProperty("hazelcast.socket.client.receive.buffer.size", -1);

    /**
     * Overrides send buffer size for connections opened by clients.
     * <p>
     * Hazelcast creates all connections with send buffer size set according to
     * {@link #SOCKET_SEND_BUFFER_SIZE}.
     * When it detects a connection was opened by a client then it adjusts send
     * buffer size according to this property.
     * <p>
     * Size is in kilobytes.
     * <p>
     * The default is -1 (same as receive buffer size for connections opened by members).
     */
    public static final HazelcastProperty SOCKET_CLIENT_SEND_BUFFER_SIZE
            = new HazelcastProperty("hazelcast.socket.client.send.buffer.size", -1);

    /**
     * If the bytebuffers used in the socket should be a direct bytebuffer ({@code true})
     * or a regular bytebuffer ({@code false}).
     */
    public static final HazelcastProperty SOCKET_CLIENT_BUFFER_DIRECT
            = new HazelcastProperty("hazelcast.socket.client.buffer.direct", false);

    /**
     * Set socket SO_LINGER option.
     */
    public static final HazelcastProperty SOCKET_LINGER_SECONDS
            = new HazelcastProperty("hazelcast.socket.linger.seconds", -1, SECONDS);

    /**
     * Socket connection timeout in seconds. Socket.connect() is blocked until
     * either connection is established or connection is refused or this timeout
     * passes. Default is 10, 0 means infinite.
     */
    public static final HazelcastProperty SOCKET_CONNECT_TIMEOUT_SECONDS
            = new HazelcastProperty("hazelcast.socket.connect.timeout.seconds", 10, SECONDS);

    /**
     * Socket set keep alive (SO_KEEPALIVE).
     */
    public static final HazelcastProperty SOCKET_KEEP_ALIVE
            = new HazelcastProperty("hazelcast.socket.keep.alive", true);

    /**
     * Socket set TCP no delay.
     */
    public static final HazelcastProperty SOCKET_NO_DELAY
            = new HazelcastProperty("hazelcast.socket.no.delay", true);

    /**
     * The number of threads doing socket input and the number of threads doing
     * socket output.
     * <p>
     * E.g., if 3 is configured, then you get 3 threads doing input and 3 doing
     * output. For individual control, check {@link #IO_INPUT_THREAD_COUNT} and
     * {@link #IO_OUTPUT_THREAD_COUNT}.
     * <p>
     * The default is depends on the number of available processors. If the
     * available processors count is smaller than 20, there will be 3+3 io threads,
     * otherwise 4+4.
     * <p>
     * If SSL is enabled, then the default number of IO threads will be corecount/2.
     */
    @SuppressWarnings("AnonInnerLength")
    public static final HazelcastProperty IO_THREAD_COUNT
            = new HazelcastProperty("hazelcast.io.thread.count", new Function<HazelcastProperties, Integer>() {
        @Override
        public Integer apply(HazelcastProperties properties) {
            return isSSLDetected(properties) ? getWhenSSLDetected() : getWhenNoSSLDetected();
        }

        private boolean isSSLDetected(HazelcastProperties properties) {
            Config config = properties.getConfig();
            if (config == null) {
                return false;
            }

            return isSSLDetected(config.getAdvancedNetworkConfig()) || isSSLDetected(config.getNetworkConfig());
        }

        private boolean isSSLDetected(AdvancedNetworkConfig networkConfig) {
            if (networkConfig == null || !networkConfig.isEnabled()) {
                return false;
            }

            for (Map.Entry<EndpointQualifier, EndpointConfig> entry : networkConfig.getEndpointConfigs().entrySet()) {
                EndpointQualifier endpointQualifier = entry.getKey();
                if (!endpointQualifier.equals(EndpointQualifier.MEMBER) && !entry.getKey().equals(EndpointQualifier.CLIENT)) {
                    continue;
                }
                EndpointConfig endpointConfig = entry.getValue();
                SSLConfig endpointSSLConfig = endpointConfig.getSSLConfig();
                if (endpointSSLConfig != null && endpointSSLConfig.isEnabled()) {
                    return true;
                }
            }
            return false;
        }

        private boolean isSSLDetected(NetworkConfig networkConfig) {
            if (networkConfig == null) {
                return false;
            }

            SSLConfig sslConfig = networkConfig.getSSLConfig();
            return sslConfig != null && sslConfig.isEnabled();
        }

        private int getWhenSSLDetected() {
            return max(getWhenNoSSLDetected(), RuntimeAvailableProcessors.get() / 2);
        }

        private int getWhenNoSSLDetected() {
            return RuntimeAvailableProcessors.get() >= 20 ? 4 : 3;
        }
    });

    /**
     * Controls the number of socket input threads. By default it is the same
     * as {@link #IO_THREAD_COUNT}.
     */
    public static final HazelcastProperty IO_INPUT_THREAD_COUNT
            = new HazelcastProperty("hazelcast.io.input.thread.count", IO_THREAD_COUNT);

    /**
     * Controls the number of socket output threads. By default it is the same
     * as {@link #IO_THREAD_COUNT}.
     */
    public static final HazelcastProperty IO_OUTPUT_THREAD_COUNT
            = new HazelcastProperty("hazelcast.io.output.thread.count", IO_THREAD_COUNT);

    /**
     * Optimization that allows sending of packets over the network to be done
     * on the calling thread if the conditions are right. This can reduce latency
     * and increase performance for low threaded environments.
     * <p>
     * It is enabled by default.
     */
    public static final HazelcastProperty IO_WRITE_THROUGH_ENABLED
            = new HazelcastProperty("hazelcast.io.write.through", true);

    /**
     * Property needed for concurrency detection so that write through can be
     * done correctly.
     * This property sets the window the concurrency detection will signalling
     * that concurrency has been detected, even if there are no further updates
     * in that window.
     * <p>
     * Normally in a concurrent system the window keeps sliding forward so it
     * will always remain concurrent.
     * <p>
     * Setting it too high effectively disables the optimization because once
     * concurrency has been detected it will keep that way. Setting it too low
     * could lead to suboptimal performance because the system will try write
     * through and other optimizations even though the system is concurrent.
     */
    public static final HazelcastProperty CONCURRENT_WINDOW_MS
            = new HazelcastProperty("hazelcast.concurrent.window.ms", 100, MILLISECONDS);

    /**
     * Minimum interval in milliseconds to consider a connection error as critical.
     */
    public static final HazelcastProperty CONNECTION_MONITOR_INTERVAL
            = new HazelcastProperty("hazelcast.connection.monitor.interval", 100, MILLISECONDS);

    /**
     * Maximum I/O error count before disconnecting from a member.
     */
    public static final HazelcastProperty CONNECTION_MONITOR_MAX_FAULTS
            = new HazelcastProperty("hazelcast.connection.monitor.max.faults", 3);

    /**
     * Client protocol message size limit (in bytes) for unverified connections
     * (i.e. maximal length of authentication message).
     */
    public static final HazelcastProperty CLIENT_PROTOCOL_UNVERIFIED_MESSAGE_BYTES =
            new HazelcastProperty("hazelcast.client.protocol.max.message.bytes", 4096);

    /*
     * CLUSTER / MEMBERSHIP / JOIN / DISCOVERY / PARTITIONING PROPERTIES
     */
    /**
     * Total number of partitions in the Hazelcast cluster.
     */
    public static final HazelcastProperty PARTITION_COUNT
            = new HazelcastProperty("hazelcast.partition.count", 271);

    /**
     * Initial expected cluster size to wait before member to start completely.
     */
    public static final HazelcastProperty INITIAL_MIN_CLUSTER_SIZE
            = new HazelcastProperty("hazelcast.initial.min.cluster.size", 0);

    /**
     * Initial time in seconds to wait before member to start completely.
     */
    public static final HazelcastProperty INITIAL_WAIT_SECONDS
            = new HazelcastProperty("hazelcast.initial.wait.seconds", 0, SECONDS);

    /**
     * The number of incremental ports, starting with port number defined in
     * network configuration, that will be used to connect to a host which is
     * defined without a port in the TCP-IP member list while a node is searching
     * for a cluster.
     */
    public static final HazelcastProperty TCP_JOIN_PORT_TRY_COUNT
            = new HazelcastProperty("hazelcast.tcp.join.port.try.count", 3);

    /**
     * Allows explicitly control if the {@link java.net.MulticastSocket#setInterface(java.net.InetAddress)} method is called in
     * the Hazelcast multicast discovery service. This configuration may affect the multicast behavior on some platforms. The
     * default value is not specified here and in such case Hazelcast multicast service itself decides if the
     * {@code setInterface()} call should be called.
     */
    public static final HazelcastProperty MULTICAST_SOCKET_SET_INTERFACE
            = new HazelcastProperty("hazelcast.multicast.socket.set.interface");

    /**
     * IP address of a multicast group. If not set, then the configuration is read from the
     * {@link com.hazelcast.config.MulticastConfig} configuration.
     */
    public static final HazelcastProperty MULTICAST_GROUP
            = new HazelcastProperty("hazelcast.multicast.group");

    /**
     * Timeout to connect all other cluster members when a member is joining to a cluster.
     */
    public static final HazelcastProperty CONNECT_ALL_WAIT_SECONDS
            = new HazelcastProperty("hazelcast.connect.all.wait.seconds", 120, SECONDS);

    /**
     * <p>Enables the Discovery SPI lookup</p>
     * <p>Discovery SPI is <b>disabled</b> by default</p>
     */
    public static final HazelcastProperty DISCOVERY_SPI_ENABLED
            = new HazelcastProperty("hazelcast.discovery.enabled", false);

    /**
     * <p>Enables the Discovery Joiner to use public IP from DiscoveredNode.</p>
     * <p>Discovery SPI is <b>disabled</b> by default</p>
     */
    public static final HazelcastProperty DISCOVERY_SPI_PUBLIC_IP_ENABLED
            = new HazelcastProperty("hazelcast.discovery.public.ip.enabled", false);

    /**
     * The delay until the first run of the split-brain handler.
     */
    public static final HazelcastProperty MERGE_FIRST_RUN_DELAY_SECONDS
            = new HazelcastProperty("hazelcast.merge.first.run.delay.seconds", 300, SECONDS);

    /**
     * The interval between invocations of the split-brain handler.
     */
    public static final HazelcastProperty MERGE_NEXT_RUN_DELAY_SECONDS
            = new HazelcastProperty("hazelcast.merge.next.run.delay.seconds", 120, SECONDS);

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

    /**
     * When enabled, which is the default behavior, partitions are migrated/replicated
     * in small fragments instead of one big chunk. Migrating partitions in fragments
     * reduces pressure on the memory and network since smaller packets are created
     * in the memory and sent through the network. Note that it can increase the
     * migration time to complete.
     */
    public static final HazelcastProperty PARTITION_FRAGMENTED_MIGRATION_ENABLED
            = new HazelcastProperty("hazelcast.partition.migration.fragments.enabled", true);

    /**
     * Enable to subdivide fragments into chunks.
     * <p>
     * Default enabled.
     *
     * @see ClusterProperty#PARTITION_FRAGMENTED_MIGRATION_ENABLED
     */
    public static final HazelcastProperty PARTITION_CHUNKED_MIGRATION_ENABLED
            = new HazelcastProperty("hazelcast.partition.migration.chunks.enabled", true);

    /**
     * Total size of all chunks in bytes during a single partition migration.
     * <p>
     * If you have parallel migrations, max migrating data
     * equals number-of-parallel-migrations times this value.
     * <p>
     * Default is 250 MB.
     */
    public static final HazelcastProperty PARTITION_CHUNKED_MAX_MIGRATING_DATA_IN_MB
            = new HazelcastProperty("hazelcast.partition.migration.chunks.max.migrating.data.in.mb", 250);

    /**
     * The time that a newly-appointed master node waits before forming a cluster.
     * Once a cluster is being started, a newly-appointed master node may receive
     * a flurry of join requests. If these requests were processed
     * immediately, the master node would have the overhead of processing each join
     * request individually and informing the existing cluster members of the
     * membership changes. Instead, the master will wait for some time to "batch"
     * the initial join requests.
     * This parameter defines time that the master node will wait since the last
     * received join request (a pre-join window) before it starts processing the
     * join requests and forming a cluster.
     * Alternatively, if the pre-join phase has laster for over
     * {@link #MAX_WAIT_SECONDS_BEFORE_JOIN} seconds, the master node will proceed
     * with processing the join requests and forming the cluster, regardless of the
     * time elapsed since the last join request.
     * <p>
     * After the master node has started processing join requests, it will no longer
     * introduce any more waiting and join requests will be processed as they arrive.
     * The only exception to this is if the master state is reset, which may happen
     * in some cases like split-brain healing.
     */
    public static final HazelcastProperty WAIT_SECONDS_BEFORE_JOIN
            = new HazelcastProperty("hazelcast.wait.seconds.before.join", 5, SECONDS);

    /**
     * The maximum time that a newly-appointed master node waits before forming a
     * cluster. Once a cluster is being started, a newly-appointed master node may
     * receive a flurry of join requests. If these requests were processed
     * immediately, the master node would have the overhead of processing each join
     * request individually and informing the existing cluster members of the
     * membership changes. Instead, the master will wait for some time to "batch"
     * the initial join requests.
     * This parameter defines maximum time that the master node will wait since the
     * first received join request (a pre-join window) before it starts processing
     * the join requests and forming a cluster.
     * This is an upper limit on the cluster’s pre-join phase duration.
     * Alternatively, if {@link #WAIT_SECONDS_BEFORE_JOIN} seconds have passed since
     * the last join request, the master node will proceed with processing the join
     * requests and forming the cluster, regardless of the time elapsed since the
     * first join request.
     * <p>
     * After the master node has started processing join requests, it will no longer
     * introduce any more waiting and join requests will be processed as they arrive.
     * The only exception to this is if the master state is reset, which may happen
     * in some cases like split-brain healing.
     */
    public static final HazelcastProperty MAX_WAIT_SECONDS_BEFORE_JOIN
            = new HazelcastProperty("hazelcast.max.wait.seconds.before.join", 20, SECONDS);

    /**
     * Join timeout, maximum time to try to join before giving up.
     */
    public static final HazelcastProperty MAX_JOIN_SECONDS
            = new HazelcastProperty("hazelcast.max.join.seconds", 300, SECONDS);

    /**
     * Split-brain merge timeout for a specific target.
     */
    public static final HazelcastProperty MAX_JOIN_MERGE_TARGET_SECONDS
            = new HazelcastProperty("hazelcast.max.join.merge.target.seconds", 20, SECONDS);

    /**
     * The interval at which member heartbeat messages are sent
     */
    public static final HazelcastProperty HEARTBEAT_INTERVAL_SECONDS
            = new HazelcastProperty("hazelcast.heartbeat.interval.seconds", 5, SECONDS);

    /**
     * The timeout which defines when master candidate gives up waiting for
     * response to its mastership claim.
     * After timeout happens, non-responding member will be removed from member
     * list.
     */
    public static final HazelcastProperty MASTERSHIP_CLAIM_TIMEOUT_SECONDS
            = new HazelcastProperty("hazelcast.mastership.claim.timeout.seconds", 120, SECONDS);

    /**
     * The timeout which defines when a cluster member is removed because it has
     * not sent any heartbeats.
     */
    public static final HazelcastProperty MAX_NO_HEARTBEAT_SECONDS
            = new HazelcastProperty("hazelcast.max.no.heartbeat.seconds", 60, SECONDS);

    /**
     * The master member, i.e, the first member in the cluster member list
     * administrates the cluster and kicks unreachable members with the heartbeat
     * mechanism. It means that a non-master member (i.e, any member other than
     * the master) does not send heartbeats to the master for the "heartbeat timeout"
     * duration, it is kicked from the cluster. However, there can be heartbeat
     * problems between non-master members as well. Since the master member is
     * the single authority to update the cluster member list, non-master members
     * report their heartbeat problems to the master so that the master can update
     * the cluster member list.
     * <p>
     * When the master receives a heartbeat problem report from another member,
     * it first waits for a number of heartbeat rounds to allow other members to
     * report their problems if there is any. After that, it takes all reports
     * received so far and checks if it can update the cluster member in a way
     * that the minimum number of members will be kicked from the cluster and
     * there won't be any heartbeat problem between the remaining members.
     * <p>
     * If this configuration option is set to 0, this functionality is disabled.
     * It is recommended to be set to at least 3 or 5 so that the master will
     * wait long enough to collect heartbeat problem reports. Otherwise, the
     * master member can make sub-optimal decisions.
     */
    public static final HazelcastProperty PARTIAL_MEMBER_DISCONNECTION_RESOLUTION_HEARTBEAT_COUNT
            = new HazelcastProperty("hazelcast.partial.member.disconnection.resolution.heartbeat.count", 0);

    /**
     * The partial member disconnection resolution mechanism uses a graph algorithm
     * that finds a max-clique in non-polynomial time. Since it could take a lot
     * of time to find a max-clique in a large graph, i.e, in a large cluster
     * with lots of random network disconnections, we use a timeout mechanism
     * to stop execution of the algorithm.
     */
    public static final HazelcastProperty PARTIAL_MEMBER_DISCONNECTION_RESOLUTION_ALGORITHM_TIMEOUT_SECONDS
            = new HazelcastProperty("hazelcast.partial.member.disconnection.resolution.algorithm.timeout.seconds", 5);

    /**
     * Heartbeat failure detector type. Available options are:
     * <ul>
     * <li>{@code deadline}:  A deadline based failure detector uses an absolute timeout
     * for missing/lost heartbeats. After timeout member is considered as dead/unavailable.
     * </li>
     * <li>{@code phi-accrual}: Implementation of 'The Phi Accrual Failure Detector'
     * by Hayashibara et al. as defined in their paper. Phi Accrual Failure Detector
     * is adaptive to network/environment conditions, that's why a lower
     * {@link #MAX_NO_HEARTBEAT_SECONDS} (for example 10 or 15 seconds) can be
     * used to provide faster detection of unavailable members.
     * </li>
     * </ul>
     * <p>
     * Default failure detector is <code>deadline</code>.
     */
    public static final HazelcastProperty HEARTBEAT_FAILURE_DETECTOR_TYPE
            = new HazelcastProperty("hazelcast.heartbeat.failuredetector.type", ClusterFailureDetectorType.DEADLINE.toString());

    /**
     * The interval at which the master sends the member lists are sent to other
     * non-master members.
     */
    public static final HazelcastProperty MEMBER_LIST_PUBLISH_INTERVAL_SECONDS
            = new HazelcastProperty("hazelcast.member.list.publish.interval.seconds", 60, SECONDS);

    /**
     * Time after which the member assumes the client is dead and closes its
     * connections to the client.
     */
    public static final HazelcastProperty CLIENT_HEARTBEAT_TIMEOUT_SECONDS
            = new HazelcastProperty("hazelcast.client.max.no.heartbeat.seconds", 300, SECONDS);

    /**
     * Once a member initiates cluster shutdown, it will either wait for all
     * members to leave the cluster before shutting itself down. This parameter
     * defines a timeout after which there may still be some members in the
     * cluster but the initiator will give up and shut itself down regardless.
     */
    public static final HazelcastProperty CLUSTER_SHUTDOWN_TIMEOUT_SECONDS
            = new HazelcastProperty("hazelcast.cluster.shutdown.timeout.seconds", 900, SECONDS);

    /**
     * Maximum wait in seconds during graceful shutdown.
     */
    public static final HazelcastProperty GRACEFUL_SHUTDOWN_MAX_WAIT
            = new HazelcastProperty("hazelcast.graceful.shutdown.max.wait", 600, SECONDS);

    /**
     * Interval for publishing partition table periodically to all cluster members
     * in seconds.
     */
    public static final HazelcastProperty PARTITION_TABLE_SEND_INTERVAL
            = new HazelcastProperty("hazelcast.partition.table.send.interval", 15, SECONDS);

    /**
     * Interval for syncing backup replicas in seconds.
     */
    public static final HazelcastProperty PARTITION_BACKUP_SYNC_INTERVAL
            = new HazelcastProperty("hazelcast.partition.backup.sync.interval", 30, SECONDS);

    /**
     * Maximum number of partition migrations to be executed concurrently on a member.
     * Member can be either source or target of the migration.
     * <ul>
     * <li>
     *  Having too much parallelization can increase the heap memory usage and
     *  overload the network during partition rebalance.
     * </li>
     * <li>
     *  Having less parallelization can increase the total migration completion time.
     * </li>
     * </ul>
     */
    public static final HazelcastProperty PARTITION_MAX_PARALLEL_MIGRATIONS
            = new HazelcastProperty("hazelcast.partition.max.parallel.migrations", 10);

    /**
     * Maximum number of anti-entropy partition replications to be executed on a member.
     * By default equals to {@link #PARTITION_MAX_PARALLEL_MIGRATIONS}.
     */
    public static final HazelcastProperty PARTITION_MAX_PARALLEL_REPLICATIONS
            = new HazelcastProperty("hazelcast.partition.max.parallel.replications", PARTITION_MAX_PARALLEL_MIGRATIONS);

    /**
     * Class name implementing {@link com.hazelcast.partition.PartitioningStrategy}, which
     * defines key to partition mapping.
     */
    public static final HazelcastProperty PARTITIONING_STRATEGY_CLASS
            = new HazelcastProperty("hazelcast.partitioning.strategy.class", "");

    /**
     * Time period to check if a client is still part of the cluster.
     */
    public static final HazelcastProperty CLIENT_CLEANUP_PERIOD
            = new HazelcastProperty("hazelcast.client.cleanup.period.millis", 10000, MILLISECONDS);

    /**
     * Timeout duration to decide if a client is still part of the cluster.
     * If a member can not find any connection to a client in the cluster, it
     * will clean up local resources that is owned by that client.
     */
    public static final HazelcastProperty CLIENT_CLEANUP_TIMEOUT
            = new HazelcastProperty("hazelcast.client.cleanup.timeout.millis", 120000, MILLISECONDS);

    /**
     * Override cluster version to use while node is not yet member of a cluster.
     * The cluster version assumed before joining a cluster may affect the serialization
     * format of cluster discovery &amp; join operations and its compatibility
     * with members of a cluster running on different Hazelcast codebase versions.
     * The default is to use the node's codebase version. You may need to override
     * it for your node to join a cluster running on a previous cluster version.
     */
    public static final HazelcastProperty INIT_CLUSTER_VERSION
            = new HazelcastProperty("hazelcast.init.cluster.version");


    /*
     * METRICS / MONITORING PROPERTIES
     */
    /**
     * Enables/disables metrics collection altogether. This is a master
     * switch for all metrics related functionality.
     * <p>
     * NOTE: This property overrides {@link MetricsConfig#isEnabled()}.
     * <p>
     * Using {@link MetricsConfig#setEnabled(boolean)} and the declarative
     * counterparts are preferred over using this property. The main purpose
     * of making metrics collection configurable from properties too is
     * allowing operators to configure the metrics subsystem from the outside
     * during investigation without touching or copying the configuration
     * potentially embedded into a signed artifact.
     */
    public static final HazelcastProperty METRICS_ENABLED
            = new HazelcastProperty("hazelcast.metrics.enabled");

    /**
     * Enables/disables collecting metrics for Management Center. If disabled,
     * Management Center can't consume the metrics from this member.
     * <p>
     * NOTE: This property overrides {@link MetricsManagementCenterConfig#isEnabled()}.
     * <p>
     * Using {@link MetricsManagementCenterConfig#setEnabled(boolean)} and the declarative
     * counterparts are preferred over using this property. The main purpose
     * of making metrics collection configurable from properties too is
     * allowing operators to configure the metrics subsystem from the outside
     * during investigation without touching or copying the configuration
     * potentially embedded into a signed artifact.
     */
    public static final HazelcastProperty METRICS_MC_ENABLED
            = new HazelcastProperty("hazelcast.metrics.mc.enabled");

    /**
     * Sets the duration in seconds for which the collected metrics are retained
     * and Management Center can consume them.
     * <p>
     * NOTE: This property overrides {@link MetricsManagementCenterConfig#getRetentionSeconds()}.
     * <p>
     * Using {@link MetricsManagementCenterConfig#setRetentionSeconds(int)} and the declarative
     * counterparts are preferred over using this property. The main purpose
     * of making metrics collection configurable from properties too is
     * allowing operators to configure the metrics subsystem from the outside
     * during investigation without touching or copying the configuration
     * potentially embedded into a signed artifact.
     */
    public static final HazelcastProperty METRICS_MC_RETENTION
            = new HazelcastProperty("hazelcast.metrics.mc.retention");

    /**
     * Enables/disables exposing metrics on JMX.
     * <p>
     * NOTE: This property overrides {@link MetricsJmxConfig#isEnabled()}.
     * <p>
     * Using {@link MetricsJmxConfig#setEnabled(boolean)} and the declarative
     * counterparts are preferred over using this property. The main purpose
     * of making metrics collection configurable from properties too is
     * allowing operators to configure the metrics subsystem from the outside
     * during investigation without touching or copying the configuration
     * potentially embedded into a signed artifact.
     */
    public static final HazelcastProperty METRICS_JMX_ENABLED
            = new HazelcastProperty("hazelcast.metrics.jmx.enabled");

    /**
     * Health monitoring log level. When SILENT, logs are printed only when values
     * exceed some predefined threshold. When NOISY, logs are always printed
     * periodically. Set OFF to turn off completely.
     */
    public static final HazelcastProperty HEALTH_MONITORING_LEVEL
            = new HazelcastProperty("hazelcast.health.monitoring.level", HealthMonitorLevel.SILENT.toString());

    /**
     * Health monitoring logging interval in seconds.
     */
    public static final HazelcastProperty HEALTH_MONITORING_DELAY_SECONDS
            = new HazelcastProperty("hazelcast.health.monitoring.delay.seconds", 20, SECONDS);

    /**
     * When the health monitoring level is SILENT, logs are printed only when the
     * memory usage exceeds this threshold.
     */
    public static final HazelcastProperty HEALTH_MONITORING_THRESHOLD_MEMORY_PERCENTAGE
            = new HazelcastProperty("hazelcast.health.monitoring.threshold.memory.percentage", 70);

    /**
     * When the health monitoring level is SILENT, logs are printed only when
     * the CPU usage exceeds this threshold.
     */
    public static final HazelcastProperty HEALTH_MONITORING_THRESHOLD_CPU_PERCENTAGE
            = new HazelcastProperty("hazelcast.health.monitoring.threshold.cpu.percentage", 70);

    /**
     * Enable JMX agent.
     */
    public static final HazelcastProperty ENABLE_JMX
            = new HazelcastProperty("hazelcast.jmx", false);

    /**
     * Some JMX MBeans are cached (e.g. map, multimap, queue and replicated map)
     * to reduce the overhead of calculating statistics. This parameter determines
     * how much the MBeans can go stale, meaning after how much time elapses since
     * the last calculated statistics should the MBean be refreshed.
     */
    public static final HazelcastProperty JMX_UPDATE_INTERVAL_SECONDS
            = new HazelcastProperty("hazelcast.jmx.update.interval.seconds", 5, SECONDS);

    /**
     * Management Center maximum visible slow operations count.
     */
    public static final HazelcastProperty MC_MAX_VISIBLE_SLOW_OPERATION_COUNT
            = new HazelcastProperty("hazelcast.mc.max.visible.slow.operations.count", 10);

    /**
     * The number of threads that the Management Center service has available
     * for processing operations sent from connected Management Center instance.
     */
    public static final HazelcastProperty MC_EXECUTOR_THREAD_COUNT
            = new HazelcastProperty("hazelcast.mc.executor.thread.count", 2);

    /**
     * Enables collecting debug metrics.
     */
    public static final HazelcastProperty METRICS_DEBUG
            = new HazelcastProperty("hazelcast.metrics.debug.enabled");

    /**
     * Enables/disables collecting metrics from the distributed data structures.
     * By default, data structure metrics collection is enabled.
     */
    public static final HazelcastProperty METRICS_DATASTRUCTURES
            = new HazelcastProperty("hazelcast.metrics.datastructures.enabled", "true");

    /**
     * Sets the metrics collection frequency in seconds.
     * <p>
     * NOTE: This property overrides {@link MetricsConfig#getCollectionFrequencySeconds()}.
     * <p>
     * Using {@link MetricsConfig#setCollectionFrequencySeconds(int)} and the declarative
     * counterparts are preferred over using this property. The main purpose
     * of making metrics collection configurable from properties too is
     * allowing operators to configure the metrics subsystem from the outside
     * during investigation without touching or copying the configuration
     * potentially embedded into a signed artifact.
     */
    public static final HazelcastProperty METRICS_COLLECTION_FREQUENCY
            = new HazelcastProperty("hazelcast.metrics.collection.frequency");

    /**
     * Enables or disables the
     * {@link com.hazelcast.spi.impl.operationexecutor.slowoperationdetector.SlowOperationDetector}.
     */
    public static final HazelcastProperty SLOW_OPERATION_DETECTOR_ENABLED
            = new HazelcastProperty("hazelcast.slow.operation.detector.enabled", true);

    /**
     * Defines a threshold above which a running operation in {@link OperationService}
     * is considered to be slow.
     * These operations will log a warning and will be shown in the
     * Management Center with detailed information, e.g. stacktrace.
     */
    public static final HazelcastProperty SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS
            = new HazelcastProperty("hazelcast.slow.operation.detector.threshold.millis", 10000, MILLISECONDS);

    /**
     * This value defines the retention time of invocations in slow operation logs.
     * <p>
     * If an invocation is older than this value, it will be purged from the log
     * to prevent unlimited memory usage. When all invocations are purged from
     * a log, the log itself will be deleted.
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
     * Defines if the stacktraces of slow operations are logged in the log file.
     * Stacktraces will always be reported to the Management Center, but by
     * default they are not printed to keep the log size small.
     */
    public static final HazelcastProperty SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED
            = new HazelcastProperty("hazelcast.slow.operation.detector.stacktrace.logging.enabled", false);

    /*
     * QUERY ENGINE PROPERTIES
     */
    /**
     * Run Query Evaluations for multiple partitions in parallel.
     * <p>
     * Each Hazelcast member evaluates query predicates using a single thread by
     * default. In most cases the overhead of inter-thread communication overweight
     * benefit of parallel execution.
     * <p>
     * When you have a large dataset and/or slow predicate you may benefit from
     * parallel predicate evaluations.
     * Set to {@code true} if you are using slow predicates or have &lt; 100,000s
     * entries per member.
     * <p>
     * The default is {@code false}.
     */
    public static final HazelcastProperty QUERY_PREDICATE_PARALLEL_EVALUATION
            = new HazelcastProperty("hazelcast.query.predicate.parallel.evaluation", false);

    /**
     * Run aggregation accumulation for multiple entries in parallel.
     * <p>
     * Each Hazelcast member executes the accumulation stage of an aggregation
     * using a single thread by default. In most cases it pays off to do it in
     * parallel.
     * <p>
     * The default is {@code true}.
     */
    public static final HazelcastProperty AGGREGATION_ACCUMULATION_PARALLEL_EVALUATION
            = new HazelcastProperty("hazelcast.aggregation.accumulation.parallel.evaluation", true);

    /**
     * Enables use of the concurrent HD index introduced in 4.1 for the IMDG
     * predicate API (old query engine) and reverts to using the per-partition
     * indexes. You may disable this property in case you encounter any
     * regressions when using the query API and when upgrading to 4.1.
     * <p>
     * The newly introduced SQL engine (see {@link com.hazelcast.sql.SqlService})
     * will still use the concurrent HD index since it cannot use the partitioned
     * indexes.
     *
     * @since 4.1
     */
    public static final HazelcastProperty GLOBAL_HD_INDEX_ENABLED
            = new HazelcastProperty("hazelcast.hd.global.index.enabled", true);

    /**
     * Result size limit for query operations on maps.
     * <p>
     * This value defines the maximum number of returned elements for a single
     * query result. If a query exceeds this number of elements, a
     * {@link QueryResultSizeExceededException} will be thrown.
     * <p>
     * This feature prevents an OOME if a single node is requesting the whole
     * data set of the cluster, such as by executing a query with
     * {@link Predicates#alwaysTrue()} predicate. This applies internally for
     * the {@link IMap#values()}, {@link IMap#keySet()} and {@link IMap#entrySet()}
     * methods, which are good candidates for OOME in large clusters.
     * <p>
     * This feature depends on an equal distribution of the data on the cluster
     * nodes to calculate the result size limit per node.
     * Therefore, there is a minimum value of
     * {@value QueryResultSizeLimiter#MINIMUM_MAX_RESULT_LIMIT} defined in
     * {@link QueryResultSizeLimiter}. Configured values below the minimum will
     * be increased to the minimum.
     * <p>
     * The feature can be disabled by setting its value to {@code -1} (which is
     * the default value).
     */
    public static final HazelcastProperty QUERY_RESULT_SIZE_LIMIT
            = new HazelcastProperty("hazelcast.query.result.size.limit", -1);

    /**
     * Maximum value of local partitions to trigger local pre-check for
     * {@link Predicates#alwaysTrue()} predicate query operations on maps.
     * <p>
     * To limit the result size of a query ({@link #QUERY_RESULT_SIZE_LIMIT});
     * a local pre-check on the requesting node can be done before the query
     * is sent to the cluster. Since this may increase the latency, the
     * pre-check is limited to a maximum number of local partitions.
     * <p>
     * By increasing this parameter, you can prevent the execution of the query
     * on the cluster. Increasing this parameter increases the latency due to
     * the prolonged local pre-check.
     * <p>
     * The pre-check can be disabled by setting the value to {@code -1}.
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
     * <p>
     * Values are case sensitive
     */
    public static final HazelcastProperty QUERY_OPTIMIZER_TYPE
            = new HazelcastProperty("hazelcast.query.optimizer.type", QueryOptimizerFactory.Type.RULES.toString());

    /**
     * Type of Query Index result copying behavior. Defines the behavior for
     * index copying on index read/write.
     * Supported in BINARY and OBJECT in-memory-formats. Ignored in NATIVE
     * in-memory-format.
     * Why is it needed? In order to support correctness the internal data-structures
     * used by indexes need to do some copying.
     * The copying may take place on-read or on-write:
     * <p>
     * -&gt; Copying on-read means that each index-read operation will copy the
     * result of the query before returning it to the caller.This copying may be
     * expensive, depending on the size of the result, since the result is stored
     * in a map, which means that all entries need to have the hash calculated
     * before being stored in a bucket. Each index-write operation however will be
     * fast, since there will be no copying taking place.
     * <p>
     * -&gt; Copying on-write means that each index-write operation will completely
     * copy the underlying map to provide the copy-on-write semantics. Depending on
     * the index size, it may be a very expensive operation. Each index-read
     * operation will be very fast, however, since it may just access the map and
     * return it to the caller.
     * <p>
     * -&gt; Never copying is tricky. It means that the internal data structures
     * of the index are concurrently modified without copy-on-write semantics.
     * Index reads never copy the results of a query to a separate map. It means
     * that the results backed by the underlying index-map can change after the
     * query has been executed. Specifically an entry might have been added / removed
     * from an index, or it might have been remapped.
     * Should be used in cases when a the caller expects "mostly correct"
     * results - specifically, if it's ok if some entries returned in the result
     * set do not match the initial query criteria.
     * The fastest solution for read and writes, since no copying takes place.
     * <p>
     * It's a tuneable trade-off - the user may decide.
     * <p>
     * Valid Values:
     * <ul>
     * <li>COPY_ON_READY - Internal data structures of the index are concurrently
     * modified without copy-on-write semantics. Index queries copy the results
     * of a query on index read to detach the result from the source map.
     * Should be used in index-write intensive cases, since the reads will slow
     * down due to the copying. Default value.
     * </li>
     * <li>COPY_ON_WRITE - Internal data structures of the index are modified
     * with copy-on-write semantics. Previously returned index query results
     * reflect the state of the index at the time of the query and are not
     * affected by future index modifications. Should be used in index-read
     * intensive cases, since the writes will slow down due to the copying.
     * </li>
     * <li>NEVER - Internal data structures of the index are concurrently modified
     * without copy-on-write semantics. Index reads never copy the results of a
     * query to a separate map. It means that the results backed by the underlying
     * index-map can change after the query has been executed. Specifically an
     * entry might have been added / removed from an index, or it might have been
     * remapped. Should be used in cases when a the caller expects "mostly correct"
     * results - specifically, if it's ok if some entries returned in the result
     * set do not match the initial query criteria.
     * The fastest solution for read and writes, since no copying takes place.
     * </li>
     * </ul>
     */
    public static final HazelcastProperty INDEX_COPY_BEHAVIOR
            = new HazelcastProperty("hazelcast.index.copy.behavior", IndexCopyBehavior.COPY_ON_READ.toString());

    /**
     * The number of threads that the client engine has available for processing
     * requests that are related to the query engine.
     * When not set it is set as core-size.
     */
    public static final HazelcastProperty CLIENT_ENGINE_QUERY_THREAD_COUNT
            = new HazelcastProperty("hazelcast.clientengine.query.thread.count", -1);

    /*
     * WAN REPLICATION PROPERTIES
     */
    /**
     * Defines the pending invocation threshold for the Enterprise WAN
     * replication implementation. Exceeding this threshold on a WAN
     * consumer member makes the member delaying the WAN acknowledgment,
     * thus slowing down the WAN publishers on the source side that send
     * WAN events to the given WAN consumer. Setting this value to
     * negative turns off the acknowledgement delaying feature.
     *
     * @see #WAN_CONSUMER_ACK_DELAY_BACKOFF_INIT_MS
     * @see #WAN_CONSUMER_ACK_DELAY_BACKOFF_MAX_MS
     * @see #WAN_CONSUMER_ACK_DELAY_BACKOFF_MULTIPLIER
     */
    public static final HazelcastProperty WAN_CONSUMER_INVOCATION_THRESHOLD
            = new HazelcastProperty("hazelcast.wan.consumer.invocation.threshold", 50000);

    /**
     * Defines the initial backoff delay for the Enterprise WAN
     * implementation's consumer. It is used if the acknowledgment
     * delaying feature is enabled by setting
     * {@link #WAN_CONSUMER_INVOCATION_THRESHOLD} to a non-negative value
     * and that the threshold is exceeded.
     *
     * @see #WAN_CONSUMER_INVOCATION_THRESHOLD
     * @see #WAN_CONSUMER_ACK_DELAY_BACKOFF_MAX_MS
     * @see #WAN_CONSUMER_ACK_DELAY_BACKOFF_MULTIPLIER
     */
    public static final HazelcastProperty WAN_CONSUMER_ACK_DELAY_BACKOFF_INIT_MS
            = new HazelcastProperty("hazelcast.wan.consumer.ack.delay.backoff.init", 1);

    /**
     * Defines the maximum backoff delay for the Enterprise WAN
     * implementation's consumer. It is used if the acknowledgment
     * delaying feature is enabled by setting
     * {@link #WAN_CONSUMER_INVOCATION_THRESHOLD} to a non-negative value
     * and that the threshold is exceeded.
     *
     * @see #WAN_CONSUMER_INVOCATION_THRESHOLD
     * @see #WAN_CONSUMER_ACK_DELAY_BACKOFF_INIT_MS
     * @see #WAN_CONSUMER_ACK_DELAY_BACKOFF_MULTIPLIER
     */
    public static final HazelcastProperty WAN_CONSUMER_ACK_DELAY_BACKOFF_MAX_MS
            = new HazelcastProperty("hazelcast.wan.consumer.ack.delay.backoff.max", 100);

    /**
     * Defines the multiplier (the speed of the function) for the backoff
     * delay for the Enterprise WAN implementation's consumer. It is used
     * if the acknowledgment delaying feature is enabled by setting
     * {@link #WAN_CONSUMER_INVOCATION_THRESHOLD} to a non-negative value
     * and that the threshold is exceeded.
     *
     * @see #WAN_CONSUMER_INVOCATION_THRESHOLD
     * @see #WAN_CONSUMER_ACK_DELAY_BACKOFF_INIT_MS
     * @see #WAN_CONSUMER_ACK_DELAY_BACKOFF_MAX_MS
     */
    public static final HazelcastProperty WAN_CONSUMER_ACK_DELAY_BACKOFF_MULTIPLIER
            = new HazelcastProperty("hazelcast.wan.consumer.ack.delay.backoff.multiplier", 1.5D);

    /*
     * IMAP PROPERTIES
     */
    /**
     * The maximum size of the key batch sent to the partition owners for value loading
     * and the maximum size of a key batch for which values are loaded in a single partition.
     */
    public static final HazelcastProperty MAP_LOAD_CHUNK_SIZE
            = new HazelcastProperty("hazelcast.map.load.chunk.size", 1000);

    /**
     * Scheduler delay for map tasks those are executed on backup members.
     */
    public static final HazelcastProperty MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS
            = new HazelcastProperty("hazelcast.map.replica.scheduled.task.delay.seconds", 10, SECONDS);

    /**
     * You can use MAP_EXPIRY_DELAY_SECONDS to deal with some possible
     * edge cases, such as using EntryProcessor. Without this delay, you
     * may see that an EntryProcessor running on the owner partition
     * found a key, but entry backup processor did not find it on backup,
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
     * <p>
     * Default: 1
     */
    public static final HazelcastProperty MAP_EVICTION_BATCH_SIZE
            = new HazelcastProperty("hazelcast.map.eviction.batch.size", 1);

    /**
     * Defines Near Cache invalidation event batch sending is enabled or not.
     */
    public static final HazelcastProperty MAP_INVALIDATION_MESSAGE_BATCH_ENABLED
            = new HazelcastProperty("hazelcast.map.invalidation.batch.enabled", true);

    /**
     * Defines the maximum number of Near Cache invalidation events to be drained
     * and sent to the event Near Cache in a batch.
     */
    public static final HazelcastProperty MAP_INVALIDATION_MESSAGE_BATCH_SIZE
            = new HazelcastProperty("hazelcast.map.invalidation.batch.size", 100);

    /**
     * Defines the Near Cache invalidation event batch sending frequency in seconds.
     * <p>
     * When the number of events do not come up to
     * {@link #MAP_INVALIDATION_MESSAGE_BATCH_SIZE} in the given time period
     * (which is defined by this property); those events are gathered into a
     * batch and sent to target.
     */
    public static final HazelcastProperty MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS
            = new HazelcastProperty("hazelcast.map.invalidation.batchfrequency.seconds", 10, SECONDS);

    /**
     * Setting this capacity is valid if you set {@code writeCoalescing} to {@code false}
     * (see {@link com.hazelcast.config.MapStoreConfig#setWriteCoalescing(boolean)}).
     * Otherwise its value will not be taken into account.
     * <p>
     * The per node maximum write-behind queue capacity is the total of all
     * write-behind queue sizes in a node, including backups.
     * <p>
     * The maximum value which can be set is {@link Integer#MAX_VALUE}
     */
    public static final HazelcastProperty MAP_WRITE_BEHIND_QUEUE_CAPACITY
            = new HazelcastProperty("hazelcast.map.write.behind.queue.capacity", 50000);

    /*
     * INVOCATION / OPERATION SYSTEM PROPERTIES
     */
    /**
     * Timeout to wait for a response when a remote call is sent, in milliseconds.
     */
    public static final HazelcastProperty OPERATION_CALL_TIMEOUT_MILLIS
            = new HazelcastProperty("hazelcast.operation.call.timeout.millis", 60000, MILLISECONDS);

    /**
     * If an operation has backups, this property specifies how long the
     * invocation will wait for acks from the backup replicas.
     * If acks are not received from some backups, there will not be any
     * rollback on other successful replicas.
     */
    public static final HazelcastProperty OPERATION_BACKUP_TIMEOUT_MILLIS
            = new HazelcastProperty("hazelcast.operation.backup.timeout.millis", 5000, MILLISECONDS);

    /**
     * When this configuration is enabled, if an operation has sync backups and
     * acks are not received from backup replicas in time, or the member which
     * owns primary replica of the target partition leaves the cluster, then
     * the invocation fails with {@link IndeterminateOperationStateException}.
     * However, even if the invocation fails, there will not be any rollback on
     * other successful replicas.
     */
    public static final HazelcastProperty FAIL_ON_INDETERMINATE_OPERATION_STATE
            = new HazelcastProperty("hazelcast.operation.fail.on.indeterminate.state", false);

    /**
     * Maximum number of retries for an invocation. After threshold is reached,
     * invocation is assumed as failed.
     */
    public static final HazelcastProperty INVOCATION_MAX_RETRY_COUNT
            = new HazelcastProperty("hazelcast.invocation.max.retry.count", InvocationBuilder.DEFAULT_TRY_COUNT);

    /**
     * Pause time between each retry cycle of an invocation in milliseconds.
     */
    public static final HazelcastProperty INVOCATION_RETRY_PAUSE
            = new HazelcastProperty("hazelcast.invocation.retry.pause.millis",
            InvocationBuilder.DEFAULT_TRY_PAUSE_MILLIS, MILLISECONDS);

    /**
     * Using back pressure, you can prevent an overload of pending asynchronous
     * backups. With a map with a single asynchronous backup, producing asynchronous
     * backups could happen at a higher rate than the consumption of the backup.
     * This can eventually lead to an OOME (especially if the backups are slow).
     * <p>
     * With back-pressure enabled, this can't happen.
     * <p>
     * Back pressure is implemented by making asynchronous backups operations
     * synchronous. This prevents the internal queues from overflowing because
     * the invoker will wait for the primary and for the backups to complete.
     * The frequency of this is determined by the sync-window.
     * <p>
     * To deal with overloads of backups, the property
     * {@link #OPERATION_BACKUP_TIMEOUT_MILLIS} should be set to a larger value;
     * above 60000 is recommended. Otherwise it can still happen backups
     * accumulate.
     */
    public static final HazelcastProperty BACKPRESSURE_ENABLED
            = new HazelcastProperty("hazelcast.backpressure.enabled", false);

    /**
     * Controls the frequency of a BackupAwareOperation getting its async backups
     * converted to a sync backups. This is needed to prevent an accumulation of
     * asynchronous backups and eventually running into stability issues.
     * <p>
     * A sync window of 10 means that 1 in 10 BackupAwareOperations get their
     * async backups convert to sync backups.
     * <p>
     * A sync window of 1 means that every BackupAwareOperation get their async
     * backups converted to sync backups. 1 is also the smallest legal value for
     * the sync window.
     * <p>
     * There is some randomization going on to prevent resonance. Therefore, with
     * a sync window of n, not every Nth BackupAwareOperation operation gets its
     * async backups converted to sync.
     * <p>
     * This property only has meaning when backpressure is enabled.
     */
    public static final HazelcastProperty BACKPRESSURE_SYNCWINDOW
            = new HazelcastProperty("hazelcast.backpressure.syncwindow", 100);

    /**
     * Control the maximum timeout in millis to wait for an invocation space to
     * be available.
     * <p>
     * If an invocation can't be made because there are too many pending invocations,
     * then an exponential backoff is done to give the system time to deal with
     * the backlog of invocations. This property controls how long an invocation
     * is allowed to wait before getting a {@link com.hazelcast.core.HazelcastOverloadException}.
     * <p>
     * The value needs to be equal or larger than 0.
     */
    public static final HazelcastProperty BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS
            = new HazelcastProperty("hazelcast.backpressure.backoff.timeout.millis", 60000, MILLISECONDS);

    /**
     * The maximum number of concurrent invocations per partition.
     * <p>
     * To prevent the system from overloading, HZ can apply a constraint on the
     * number of concurrent invocations. If the maximum number of concurrent
     * invocations has been exceeded and a new invocation comes in, then an
     * exponential back-off is applied till eventually a timeout happens or there
     * is room for the invocation.
     * <p>
     * By default it is configured as 100. With 271 partitions, that would give
     * (271 + 1) * 100 = 27200 concurrent invocations from a single member. The
     * +1 is for generic operations. The reasons why 100 is chosen are:
     * - there can be concurrent operations that touch a lot of partitions which
     * consume more than 1 invocation, and
     * - certain methods like those from the IExecutor are also invocations and
     * they can be very long running.
     * <p>
     * No promise is made for the invocations being tracked per partition, or
     * if there is a general pool of invocations.
     */
    public static final HazelcastProperty BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION
            = new HazelcastProperty("hazelcast.backpressure.max.concurrent.invocations.per.partition", 100);

    /**
     * Hazelcast allows read operations to be performed while a partition is
     * being migrated. This can lead to stale reads for some scenarios. You can
     * disable stale read operations by setting this system property’s value to
     * "true". Its default value is "false", meaning that stale reads are allowed.
     */
    public static final HazelcastProperty DISABLE_STALE_READ_ON_PARTITION_MIGRATION
            = new HazelcastProperty("hazelcast.partition.migration.stale.read.disabled", false);

    /**
     * The number of partition operation handler threads per member.
     * <p>
     * If this is less than the number of partitions on a member, partition operations
     * will queue behind other operations of different partitions.
     */
    public static final HazelcastProperty PARTITION_OPERATION_THREAD_COUNT
            = new HazelcastProperty("hazelcast.operation.thread.count",
            (Function<HazelcastProperties, Integer>) properties -> max(2, RuntimeAvailableProcessors.get()));

    /**
     * The number of generic operation handler threads per member.
     * <p>
     * The default is max(2, processors/2);
     */
    public static final HazelcastProperty GENERIC_OPERATION_THREAD_COUNT
            = new HazelcastProperty("hazelcast.operation.generic.thread.count",
            (Function<HazelcastProperties, Integer>) o -> {
                // default generic operation thread count
                int processors = RuntimeAvailableProcessors.get();
                return max(2, processors / 2);
            });

    /**
     * The number of priority generic operation handler threads per member.
     * <p>
     * The default is 1.
     * <p>
     * Having at least 1 priority generic operation thread helps to improve cluster
     * stability since a lot of cluster operations are generic priority operations
     * and they should get executed as soon as possible. If there is a dedicated
     * generic operation thread then these operations don't get delayed because
     * the generic threads are busy executing regular user operations. So unless
     * memory consumption is an issue, make sure there is at least 1 thread.
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

    /*
     * ICACHE / JCACHE PROPERTIES
     */

    /**
     * Defines cache invalidation event batch sending is enabled or not.
     */
    public static final HazelcastProperty CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED
            = new HazelcastProperty("hazelcast.cache.invalidation.batch.enabled", true);

    /**
     * Defines the maximum number of cache invalidation events to be drained and
     * sent to the event listeners in a batch.
     */
    public static final HazelcastProperty CACHE_INVALIDATION_MESSAGE_BATCH_SIZE
            = new HazelcastProperty("hazelcast.cache.invalidation.batch.size", 100);

    /**
     * Defines the cache invalidation event batch sending frequency in seconds.
     * <p>
     * When the number of events do not come up to
     * {@link #CACHE_INVALIDATION_MESSAGE_BATCH_SIZE} in the given time period
     * (which is defined by this property); those events are gathered into a
     * batch and sent to target.
     */
    public static final HazelcastProperty CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS
            = new HazelcastProperty("hazelcast.cache.invalidation.batchfrequency.seconds", 10, SECONDS);

    /**
     * Forces the JCache provider, which can have values client or server, to
     * force the provider type. If not provided, the provider will be client or
     * server, whichever is found on the classpath first respectively.
     */
    public static final HazelcastProperty JCACHE_PROVIDER_TYPE
            = new HazelcastProperty("hazelcast.jcache.provider.type");


    /*
     * OTHER PROPERTIES
     */
    /**
     * The number of threads that the client engine has available for processing
     * requests that are not partition specific.
     * Most of the requests, such as {@code map.put} and {@code map.get}, are
     * partition specific and will use a partition-specific operation thread,
     * but there are also requests that can't be executed on a partition-specific
     * operation thread, such as {@code multimap.containsValue(value)}, because
     * they need to access all partitions on a given member.
     * <p>
     * When not set it is set as core-size.
     */
    public static final HazelcastProperty CLIENT_ENGINE_THREAD_COUNT
            = new HazelcastProperty("hazelcast.clientengine.thread.count", -1);

    /**
     * The number of threads that the client engine has available for processing requests that are blocking
     * (example: related to transactions)
     * When not set it is set as core-size * 20.
     */
    public static final HazelcastProperty CLIENT_ENGINE_BLOCKING_THREAD_COUNT
            = new HazelcastProperty("hazelcast.clientengine.blocking.thread.count", -1);

    /**
     * Number of threads for the {@link com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl}
     * executor.
     * The executor is responsible for executing the events. If you process a
     * lot of events and have many cores, setting a higher value is a good practice.
     * This way, more events can be processed in parallel.
     */
    public static final HazelcastProperty EVENT_THREAD_COUNT
            = new HazelcastProperty("hazelcast.event.thread.count", 5);

    /**
     * The capacity of the {@link com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl}
     * executor.
     * The executor is responsible for executing the events. If the events are
     * produced at a higher rate than they are consumed, the queue grows in size.
     * This can lead to an {@link OutOfMemoryError} if the accumulated events
     * are not small enough to fit in memory. This capacity is shared between
     * event topics.
     * When the maximum capacity is reached, the items are dropped. This means
     * that the event system is a 'best effort' system and there is no guarantee
     * that you are going to get an event.
     * Since the capacity is shared between topics, one topic might fill the
     * entire queue and cause other topics to drop their messages.
     */
    public static final HazelcastProperty EVENT_QUEUE_CAPACITY
            = new HazelcastProperty("hazelcast.event.queue.capacity", 1000000);

    /**
     * The timeout for offering an event to the event executor for processing.
     * If the event queue is full, the event might not be accepted to the queue
     * and it will be dropped.
     * This applies only to processing of local events. Remote events (events
     * on a remote subscriber) have no timeout, meaning that the event can be
     * rejected immediately.
     */
    public static final HazelcastProperty EVENT_QUEUE_TIMEOUT_MILLIS
            = new HazelcastProperty("hazelcast.event.queue.timeout.millis", 250, MILLISECONDS);

    /**
     * To prevent overloading of the outbound connections, once in a while an
     * event is made synchronous by wrapping it in a fake operation and waiting
     * for a fake response. This causes the outbound write queue of the connection
     * to get drained.
     * <p>
     * This timeout configures the maximum amount of waiting time for this fake
     * response. Setting it to a too low value can lead to an uncontrolled growth
     * of the outbound write queue of the connection.
     */
    public static final HazelcastProperty EVENT_SYNC_TIMEOUT_MILLIS
            = new HazelcastProperty("hazelcast.event.sync.timeout.millis", 5000, MILLISECONDS);

    /**
     * Enable or disable the sending of phone home data to Hazelcast’s phone home
     * server.
     */
    public static final HazelcastProperty PHONE_HOME_ENABLED
            = new HazelcastProperty("hazelcast.phone.home.enabled", true);

    /**
     * Enable Hazelcast shutdownhook thread. When this is enabled, this thread
     * terminates the Hazelcast instance without waiting to shutdown gracefully.
     */
    public static final HazelcastProperty SHUTDOWNHOOK_ENABLED
            = new HazelcastProperty("hazelcast.shutdownhook.enabled", true);

    /**
     * Behaviour when JVM is about to exit while Hazelcast instance is still running.
     * <p>
     * Possible values:
     * TERMINATE: Terminate Hazelcast immediately
     * GRACEFUL:  Initiate graceful shutdown. This can significantly slow-down
     * JVM exit process, but it's tries to retain data safety.
     * <p>
     * Default: TERMINATE
     * <p>
     * You should always shutdown Hazelcast explicitly via {@link HazelcastInstance#shutdown()}
     * It's not recommended to rely on shutdown hook, this is a last-effort measure.
     */
    public static final HazelcastProperty SHUTDOWNHOOK_POLICY
            = new HazelcastProperty("hazelcast.shutdownhook.policy", "TERMINATE");

    /**
     * Name of logging framework type to send logging events.
     */
    public static final HazelcastProperty LOGGING_TYPE
            = new HazelcastProperty("hazelcast.logging.type", "jdk");

    /**
     * Controls whether cluster name, ip and version should be included in all
     * log messages.
     */
    public static final HazelcastProperty LOGGING_ENABLE_DETAILS
            = new HazelcastProperty("hazelcast.logging.details.enabled", true);

    /**
     * All locks which are acquired without an explicit lease time use this value
     * (in seconds) as the lease time. When you want to set an explicit lease
     * time for your locks, you cannot set it to a longer time than this value.
     */
    public static final HazelcastProperty LOCK_MAX_LEASE_TIME_SECONDS
            = new HazelcastProperty("hazelcast.lock.max.lease.time.seconds", Long.MAX_VALUE, SECONDS);

    /**
     * Hazelcast IMDG Enterprise license key.
     */
    public static final HazelcastProperty ENTERPRISE_LICENSE_KEY
            = new HazelcastProperty("hazelcast.enterprise.license.key");

    /**
     * Hazelcast serialization version. This is single byte value between 1 and
     * Max supported serialization version.
     *
     * @see BuildInfo#getSerializationVersion()
     */
    public static final HazelcastProperty SERIALIZATION_VERSION
            = new HazelcastProperty("hazelcast.serialization.version",
            BuildInfoProvider.getBuildInfo().getSerializationVersion());

    /**
     * By default, search for data structures config is performed within static
     * configuration first:
     * <ul>
     * <li>Exact match in static config</li>
     * <li>Wildcard match in static config</li>
     * <li>Exact match in dynamic config</li>
     * <li>Wildcard match in dynamic config</li>
     * <li>Fallback to default</li>
     * </ul>
     * But sometimes it makes sense to perform search within dynamic configs
     * first. If this property is set to
     * {@code true}, search algorithm changes to:
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

    /**
     * Defines whether Moby Names should be used for instance name generating
     * when it is not provided by user.
     * <p>
     * Moby Name is a short human-readable name consisting of randomly chosen
     * adjective and the surname of a famous person.
     * <p>
     * If set to {@code true}, Moby Name will be chosen, otherwise a name that
     * is concatenation of static prefix, number and cluster name.
     * <p>
     * By default is {@code true}.
     */
    public static final HazelcastProperty MOBY_NAMING_ENABLED
            = new HazelcastProperty("hazelcast.member.naming.moby.enabled", true);

    /*
     * JET PROPERTIES
     */
    /**
     * Jet will periodically check for new jobs to start and perform cleanup of
     * unused resources. This property configures how often this check and
     * cleanup will be done. Value is in milliseconds.
     *
     * @since Jet 3.2
     */
    public static final HazelcastProperty JOB_SCAN_PERIOD
            = new HazelcastProperty("hazelcast.jet.job.scan.period", SECONDS.toMillis(5), MILLISECONDS)
            .setDeprecatedName("jet.job.scan.period");

    /**
     * Maximum number of time in seconds the job results will be kept in
     * the cluster. They will be automatically deleted after this period
     * is reached.
     * <p>
     * Default value is 7 days.
     *
     * @since Jet 3.2
     */
    public static final HazelcastProperty JOB_RESULTS_TTL_SECONDS
            = new HazelcastProperty("hazelcast.jet.job.results.ttl.seconds", DAYS.toSeconds(7), SECONDS)
            .setDeprecatedName("jet.job.results.ttl.seconds");

    /**
     * Maximum number of job results to keep in the cluster, the oldest
     * results will be automatically deleted after this size is reached.
     * <p>
     * Default value is 1,000 jobs.
     *
     * @since Jet 3.2
     */
    public static final HazelcastProperty JOB_RESULTS_MAX_SIZE
            = new HazelcastProperty("hazelcast.jet.job.results.max.size", 1_000)
            .setDeprecatedName("jet.job.results.max.size");

    /**
     * The minimum time in microseconds the cooperative worker threads will
     * sleep if none of the tasklets made any progress. Lower values increase
     * idle CPU usage but may result in decreased latency. Higher values will
     * increase latency and very high values (>10000µs) will also limit the
     * throughput.
     * <p>
     * The default is value is {@code 25µs}.
     * <p>
     * Note: the underlying {@link LockSupport#parkNanos(long)} call may
     * actually sleep longer depending on the operating system (up to 15000µs
     * on Windows). See the <a
     * href="https://hazelcast.com/blog/locksupport-parknanos-under-the-hood-and-the-curious-case-of-parking/">
     * Hazelcast blog post about this subject</a> for more details.
     * <p>
     * See also: {@link #JET_IDLE_COOPERATIVE_MAX_MICROSECONDS}
     *
     * @since Jet 3.2
     */
    public static final HazelcastProperty JET_IDLE_COOPERATIVE_MIN_MICROSECONDS
            = new HazelcastProperty("hazelcast.jet.idle.cooperative.min.microseconds", 25, MICROSECONDS)
            .setDeprecatedName("jet.idle.cooperative.min.microseconds");

    /**
     * The maximum time in microseconds the cooperative worker threads will
     * sleep if none of the tasklets made any progress. Lower values increase
     * idle CPU usage but may result in decreased latency. Higher values will
     * increase latency and very high values (>10000µs) will also limit the
     * throughput.
     * <p>
     * The default is value is {@code 500µs}.
     * <p>
     * Note: the underlying {@link LockSupport#parkNanos(long)} call may
     * actually sleep longer depending on the operating system (up to 15000µs on
     * Windows). See the <a
     * href="https://hazelcast.com/blog/locksupport-parknanos-under-the-hood-and-the-curious-case-of-parking/">
     * Hazelcast blog post about this subject</a> for more details.
     * <p>
     * See also: {@link #JET_IDLE_COOPERATIVE_MIN_MICROSECONDS}
     *
     * @since Jet 3.2
     */
    public static final HazelcastProperty JET_IDLE_COOPERATIVE_MAX_MICROSECONDS
            = new HazelcastProperty("hazelcast.jet.idle.cooperative.max.microseconds", 500, MICROSECONDS)
            .setDeprecatedName("jet.idle.cooperative.max.microseconds");

    /**
     * The minimum time in microseconds the non-cooperative worker threads will
     * sleep if none of the tasklets made any progress. Lower values increase
     * idle CPU usage but may result in decreased latency. Higher values will
     * increase latency and very high values (>10000µs) will also limit the
     * throughput.
     * <p>
     * The default is value is {@code 25µs}.
     * <p>
     * Note: the underlying {@link LockSupport#parkNanos(long)} call may actually
     * sleep longer depending on the operating system (up to 15000µs on Windows).
     * See the <a
     * href="https://hazelcast.com/blog/locksupport-parknanos-under-the-hood-and-the-curious-case-of-parking/">
     * Hazelcast blog post about this subject</a> for more details.
     * <p>
     * See also: {@link #JET_IDLE_NONCOOPERATIVE_MAX_MICROSECONDS}
     *
     * @since Jet 3.2
     */
    public static final HazelcastProperty JET_IDLE_NONCOOPERATIVE_MIN_MICROSECONDS
            = new HazelcastProperty("hazelcast.jet.idle.noncooperative.min.microseconds", 25, MICROSECONDS)
            .setDeprecatedName("jet.idle.noncooperative.min.microseconds");

    /**
     * The maximum time in microseconds the non-cooperative worker threads will
     * sleep if none of the tasklets made any progress. Lower values increase
     * idle CPU usage but may result in decreased latency. Higher values will
     * increase latency and very high values (>10000µs) will also limit the
     * throughput.
     * <p>
     * The default is value is {@code 5000µs}.
     * <p>
     * Note: the underlying {@link LockSupport#parkNanos(long)} call may actually
     * sleep longer depending on the operating system (up to 15000µs on Windows).
     * See the <a
     * href="https://hazelcast.com/blog/locksupport-parknanos-under-the-hood-and-the-curious-case-of-parking/">
     * Hazelcast blog post about this subject</a> for more details.
     * <p>
     * See also: {@link #JET_IDLE_NONCOOPERATIVE_MIN_MICROSECONDS}
     *
     * @since Jet 3.2
     */
    public static final HazelcastProperty JET_IDLE_NONCOOPERATIVE_MAX_MICROSECONDS
            = new HazelcastProperty("hazelcast.jet.idle.noncooperative.max.microseconds", 5000, MICROSECONDS)
            .setDeprecatedName("jet.idle.noncooperative.max.microseconds");

    /**
     * The directory containing jars, that can be used to specify custom classpath for
     * a stage in a pipeline.
     * The default value is `custom-lib`, relative to the current directory.
     *
     * @since 5.0
     */
    public static final HazelcastProperty PROCESSOR_CUSTOM_LIB_DIR
            = new HazelcastProperty("hazelcast.jet.custom.lib.dir", "custom-lib");

    /**
     * Controls whether cluster emojis can be used in log messages. This is just a hint for components calling the logging.
     *
     * @since 5.0
     */
    public static final HazelcastProperty LOG_EMOJI_ENABLED = new HazelcastProperty("hazelcast.logging.emoji.enabled",
            StandardCharsets.UTF_8.equals(Charset.defaultCharset()) && !OsHelper.isWindows());

    /**
     * When set to any not-{@code null} value, security recommendations are logged on INFO level during the node start. The
     * default value is {@code null}.
     *
     * @since 5.0
     */
    public static final HazelcastProperty SECURITY_RECOMMENDATIONS = new HazelcastProperty(
            "hazelcast.security.recommendations");

    private ClusterProperty() {
    }
}
