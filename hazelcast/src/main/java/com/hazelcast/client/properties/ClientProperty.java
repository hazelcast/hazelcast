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

package com.hazelcast.client.properties;

import com.hazelcast.client.config.ClientMetricsConfig;
import com.hazelcast.config.MetricsJmxConfig;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.spi.properties.HazelcastProperty;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Defines the name and default value for Hazelcast Client properties.
 */
public final class ClientProperty {

    /**
     * Client shuffles the given member list to prevent all clients to connect to the same node when
     * this property is set to true. When it is set to false, the client tries to connect to the nodes
     * in the given order.
     */
    public static final HazelcastProperty SHUFFLE_MEMBER_LIST
            = new HazelcastProperty("hazelcast.client.shuffle.member.list", true);

    /**
     * Client sends heartbeat messages to the members and this is the timeout for this sending operations.
     * If there is not any message passing between the client and member within the given time via this property
     * in milliseconds, the connection will be closed.
     */
    public static final HazelcastProperty HEARTBEAT_TIMEOUT
            = new HazelcastProperty("hazelcast.client.heartbeat.timeout", 60000, MILLISECONDS);

    /**
     * Time interval between the heartbeats sent by the client to the nodes.
     */
    public static final HazelcastProperty HEARTBEAT_INTERVAL
            = new HazelcastProperty("hazelcast.client.heartbeat.interval", 5000, MILLISECONDS);

    /**
     * Number of the threads to handle the incoming event packets.
     */
    public static final HazelcastProperty EVENT_THREAD_COUNT
            = new HazelcastProperty("hazelcast.client.event.thread.count", 5);

    /**
     * Capacity of the executor that handles the incoming event packets.
     */
    public static final HazelcastProperty EVENT_QUEUE_CAPACITY
            = new HazelcastProperty("hazelcast.client.event.queue.capacity", 1000000);

    /**
     * When an invocation gets an exception because :
     * - Member throws an exception.
     * - Connection between the client and member is closed.
     * - Client's heartbeat requests are timed out.
     * Time passed since invocation started is compared with this property.
     * If the time is already passed, then the exception is delegated to the user. If not, the invocation is retried.
     * Note that, if invocation gets no exception and it is a long running one, then it will not get any exception,
     * no matter how small this timeout is set.
     */
    public static final HazelcastProperty INVOCATION_TIMEOUT_SECONDS
            = new HazelcastProperty("hazelcast.client.invocation.timeout.seconds", 120, SECONDS);

    /**
     * Pause time between each retry cycle of an invocation in milliseconds.
     */
    public static final HazelcastProperty INVOCATION_RETRY_PAUSE_MILLIS
            = new HazelcastProperty("hazelcast.client.invocation.retry.pause.millis", 1000, MILLISECONDS);

    /**
     * The maximum number of concurrent invocations allowed.
     * <p>
     * To prevent the system from overloading, user can apply a constraint on the number of concurrent invocations.
     * If the maximum number of concurrent invocations has been exceeded and a new invocation comes in,
     * then hazelcast will throw HazelcastOverloadException
     * <p>
     * By default it is configured as Integer.MaxValue.
     */
    public static final HazelcastProperty MAX_CONCURRENT_INVOCATIONS
            = new HazelcastProperty("hazelcast.client.max.concurrent.invocations", Integer.MAX_VALUE);

    /**
     * Control the maximum timeout in millis to wait for an invocation space to be available.
     * <p>
     * If an invocation can't be made because there are too many pending invocations, then an exponential backoff is done
     * to give the system time to deal with the backlog of invocations. This property controls how long an invocation is
     * allowed to wait before getting a {@link com.hazelcast.core.HazelcastOverloadException}.
     * <p>
     * When set to -1 then <code>HazelcastOverloadException</code> is thrown immediately without any waiting.
     */
    public static final HazelcastProperty BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS
            = new HazelcastProperty("hazelcast.client.invocation.backoff.timeout.millis", -1, MILLISECONDS);

    /**
     * <p>Enables the Discovery SPI</p>
     * <p>Discovery SPI is <b>disabled</b> by default</p>
     */
    public static final HazelcastProperty DISCOVERY_SPI_ENABLED
            = new HazelcastProperty("hazelcast.discovery.enabled", false);

    /**
     * <p>Enables the Discovery Joiner to use public IP from DiscoveredNode.</p>
     * <p>Discovery SPI is <b>disabled</b> by default</p>
     */
    public static final HazelcastProperty DISCOVERY_SPI_PUBLIC_IP_ENABLED
            = new HazelcastProperty("hazelcast.discovery.public.ip.enabled");

    /**
     * Controls the number of IO input threads. Defaults to -1, so the system will decide.
     * <p>
     * If client is a smart client and processor count larger than 8, it will default to 3 otherwise it will default to 1.
     */
    public static final HazelcastProperty IO_INPUT_THREAD_COUNT
            = new HazelcastProperty("hazelcast.client.io.input.thread.count", -1);

    /**
     * Controls the number of IO output threads. Defaults to -1, so the system will decide.
     * <p>
     * If client is a smart client and processor count larger than 8 , it will default to 3 otherwise it will default to 1.
     */
    public static final HazelcastProperty IO_OUTPUT_THREAD_COUNT
            = new HazelcastProperty("hazelcast.client.io.output.thread.count", -1);

    /**
     * The interval in seconds between {@link com.hazelcast.internal.networking.nio.iobalancer.IOBalancer IOBalancer}
     * executions. The shorter intervals will catch I/O Imbalance faster, but they will cause higher overhead.
     * <p>
     * Please see the documentation of {@link com.hazelcast.internal.networking.nio.iobalancer.IOBalancer IOBalancer}
     * for a detailed explanation of the problem.
     * <p>
     * The default is 20 seconds. A value smaller than 1 disables the balancer.
     */
    public static final HazelcastProperty IO_BALANCER_INTERVAL_SECONDS
            = new HazelcastProperty("hazelcast.client.io.balancer.interval.seconds", 20, SECONDS);

    /**
     * Optimization that allows sending of packets over the network to be done on the calling thread if the
     * conditions are right. This can reduce latency and increase performance for low threaded environments.
     * <p>
     * It is enabled by default.
     */
    public static final HazelcastProperty IO_WRITE_THROUGH_ENABLED
            = new HazelcastProperty("hazelcast.client.io.write.through", true);

    /**
     * Property needed for concurrency detection so that write through and dynamic response handling
     * can be done correctly. This property sets the window the concurrency detection will signalling
     * that concurrency has been detected, even if there are no further updates in that window.
     * <p>
     * Normally in a concurrent system the window keeps sliding forward so it will always remain
     * concurrent.
     * <p>
     * Setting it too high effectively disables the optimization because once concurrency has been detected
     * it will keep that way. Setting it too low could lead to suboptimal performance because the system
     * will try write through and other optimizations even though the system is concurrent.
     */
    public static final HazelcastProperty CONCURRENT_WINDOW_MS
            = new HazelcastProperty("hazelcast.client.concurrent.window.ms", 100, MILLISECONDS);

    /**
     * The number of response threads.
     * <p>
     * By default there are 2 response threads; this gives stable and good performance.
     * <p>
     * If set to 0, the response threads are bypassed and the response handling is done
     * on the IO threads. Under certain conditions this can give a higher throughput, but
     * setting to 0 should be regarded an experimental feature.
     * <p>
     * If set to 0, the IO_OUTPUT_THREAD_COUNT is really going to matter because the
     * inbound thread will have more work to do. By default when TLS isn't enable,
     * there is just 1 inbound thread.
     */
    public static final HazelcastProperty RESPONSE_THREAD_COUNT
            = new HazelcastProperty("hazelcast.client.response.thread.count", 2);

    /**
     * Enabled dynamic switching between processing responses on the io threads
     * and offloading the response threads.
     * <p>
     * Under certain conditions (single threaded clients) processing on the io
     * thread can increase performance because useless handover to the response
     * thread is removed. Also the response thread isn't created until it is needed
     * and especially for ephemeral clients reducing threads can lead to
     * increased performance and reduced memory usage.
     */
    public static final HazelcastProperty RESPONSE_THREAD_DYNAMIC
            = new HazelcastProperty("hazelcast.client.response.thread.dynamic", true);

    /**
     * Token to use when discovering cluster via hazelcast.cloud
     */
    public static final HazelcastProperty HAZELCAST_CLOUD_DISCOVERY_TOKEN =
            new HazelcastProperty("hazelcast.client.cloud.discovery.token");

    /**
     * If an operation has backups, this property specifies how long the invocation will wait for acks from the backup replicas.
     * If acks are not received from some backups, there will not be any rollback on other successful replicas.
     */
    public static final HazelcastProperty OPERATION_BACKUP_TIMEOUT_MILLIS
            = new HazelcastProperty("hazelcast.client.operation.backup.timeout.millis", 5000, MILLISECONDS);

    /**
     * When this configuration is enabled, if an operation has sync backups and acks are not received from backup replicas
     * in time, or the member which owns primary replica of the target partition leaves the cluster, then the invocation fails
     * with {@link IndeterminateOperationStateException}. However, even if the invocation fails,
     * there will not be any rollback on other successful replicas.
     */
    public static final HazelcastProperty FAIL_ON_INDETERMINATE_OPERATION_STATE
            = new HazelcastProperty("hazelcast.client.operation.fail.on.indeterminate.state", false);

    /**
     * Enables the client statistics collection.
     * <p>
     * The default is {@code false}.
     * <p>
     * Setting this enables Metrics since 4.0.
     * <p>
     * If both this and {@link #METRICS_ENABLED} are configured, this is
     * ignored.
     * <p>
     * Note that when this is enabled, the default value of
     * {@link #METRICS_COLLECTION_FREQUENCY} (5 seconds) will be used instead
     * of {@link #STATISTICS_PERIOD_SECONDS} (3 seconds), when not set
     * explicitly.
     * @deprecated since 4.0. Use {@link #METRICS_ENABLED}
     * ({@code "hazelcast.client.metrics.enabled"}) instead.
     */
    @Deprecated
    public static final HazelcastProperty STATISTICS_ENABLED = new HazelcastProperty("hazelcast.client.statistics.enabled",
            false);

    /**
     * The period in seconds the statistics run.
     * <p>
     * The values set here is used as {@link #METRICS_COLLECTION_FREQUENCY} as instead.
     * If both this and {@link #METRICS_COLLECTION_FREQUENCY} are configured,
     * this is ignored.
     * @deprecated since 4.0. Use {@link #METRICS_COLLECTION_FREQUENCY}
     * ({@code "hazelcast.client.metrics.collection.frequency"}) instead.
     */
    @Deprecated
    public static final HazelcastProperty STATISTICS_PERIOD_SECONDS = new HazelcastProperty(
            "hazelcast.client.statistics.period.seconds", 3, SECONDS);

    /**
     * Enables/disables metrics collection altogether. This is a master
     * switch for all metrics related functionality.
     * <p>
     * NOTE: This property overrides {@link ClientMetricsConfig#isEnabled()}.
     * <p>
     * Using {@link ClientMetricsConfig#setEnabled(boolean)} and the declarative
     * counterparts are preferred over using this property. The main purpose
     * of making metrics collection configurable from properties too is
     * allowing operators to configure the metrics subsystem from the outside
     * during investigation without touching or copying the configuration
     * potentially embedded into a signed artifact.
     */
    public static final HazelcastProperty METRICS_ENABLED
            = new HazelcastProperty("hazelcast.client.metrics.enabled");

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
            = new HazelcastProperty("hazelcast.client.metrics.jmx.enabled");

    /**
     * Enables collecting debug metrics. Debug metrics are sent to the
     * diagnostics only.
     */
    public static final HazelcastProperty METRICS_DEBUG
            = new HazelcastProperty("hazelcast.client.metrics.debug.enabled");

    /**
     * Sets the metrics collection frequency in seconds.
     * <p>
     * NOTE: This property overrides {@link ClientMetricsConfig#getCollectionFrequencySeconds()}.
     * <p>
     * Using {@link ClientMetricsConfig#setCollectionFrequencySeconds(int)} and the declarative
     * counterparts are preferred over using this property. The main purpose
     * of making metrics collection configurable from properties too is
     * allowing operators to configure the metrics subsystem from the outside
     * during investigation without touching or copying the configuration
     * potentially embedded into a signed artifact.
     */
    public static final HazelcastProperty METRICS_COLLECTION_FREQUENCY
            = new HazelcastProperty("hazelcast.client.metrics.collection.frequency", 5);


    private ClientProperty() {
    }
}
