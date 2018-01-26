/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi.properties;

import com.hazelcast.spi.properties.HazelcastProperty;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Defines the name and default value for Hazelcast Client properties.
 */
public final class ClientProperty {

    /**
     * Client shuffles the given member list to prevent all clients to connect to the same node when
     * this property is set to false. When it is set to true, the client tries to connect to the nodes
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
     * <p/>
     * To prevent the system from overloading, user can apply a constraint on the number of concurrent invocations.
     * If the maximum number of concurrent invocations has been exceeded and a new invocation comes in,
     * then hazelcast will throw HazelcastOverloadException
     * <p/>
     * By default it is configured as Integer.MaxValue.
     */
    public static final HazelcastProperty MAX_CONCURRENT_INVOCATIONS
            = new HazelcastProperty("hazelcast.client.max.concurrent.invocations", Integer.MAX_VALUE);

    /**
     * Control the maximum timeout in millis to wait for an invocation space to be available.
     * <p/>
     * If an invocation can't be made because there are too many pending invocations, then an exponential backoff is done
     * to give the system time to deal with the backlog of invocations. This property controls how long an invocation is
     * allowed to wait before getting a {@link com.hazelcast.core.HazelcastOverloadException}.
     * <p/>
     * <p>
     * When set to -1 then <code>HazelcastOverloadException</code> is thrown immediately without any waiting.
     * </p>
     */
    public static final HazelcastProperty BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS
            = new HazelcastProperty("hazelcast.client.invocation.backoff.timeout.millis", -1, MILLISECONDS);

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
     * When this property is true, if the client can not know the server version it will assume that the server is version 3.6.x.
     * This property is especially needed if you are using ICache (or JCache).
     */
    public static final HazelcastProperty COMPATIBILITY_3_6_SERVER_ENABLED
            = new HazelcastProperty("hazelcast.compatibility.3.6.server", false);


    /**
     * Controls the number of socket input threads. Defaults to -1, so the system will determine.
     *
     * If SSL is disabled, system will default to 1.
     * If SSL is enabled, system will default to 3.
     */
    public static final HazelcastProperty IO_INPUT_THREAD_COUNT
            = new HazelcastProperty("hazelcast.client.io.input.thread.count", -1);

    /**
     * Controls the number of socket output threads. Defaults to -1, so the system will determine.
     *
     * If SSL is disabled, system will default to 1.
     * If SSL is enabled, system will default to 3.
     */
    public static final HazelcastProperty IO_OUTPUT_THREAD_COUNT
            = new HazelcastProperty("hazelcast.client.io.output.thread.count", -1);

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
            = new HazelcastProperty("hazelcast.client.io.balancer.interval.seconds", 20, SECONDS);


    private ClientProperty() {
    }
}
