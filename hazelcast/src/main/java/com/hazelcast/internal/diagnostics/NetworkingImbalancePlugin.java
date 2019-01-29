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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.internal.networking.EventLoopGroup;
import com.hazelcast.internal.networking.nio.NioEventLoopGroup;
import com.hazelcast.internal.networking.nio.NioThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import static com.hazelcast.internal.diagnostics.Diagnostics.PREFIX;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The {@link NetworkingImbalancePlugin} is an experimental plugin meant for detecting imbalance in the IO system.
 * <p>
 * This  plugin will probably mostly be used for internal purposes to get a better understanding of imbalances.
 * Normally imbalances are taken care of by the IOBalancer; but we need to make sure it makes the right choice.
 * <p>
 * This plugin can be used on server and client side.
 */
public class NetworkingImbalancePlugin extends DiagnosticsPlugin {

    /**
     * The period in seconds this plugin runs.
     * <p>
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty PERIOD_SECONDS
            = new HazelcastProperty(PREFIX + ".networking-imbalance.seconds", 0, SECONDS);

    private static final double HUNDRED = 100d;

    private final NioEventLoopGroup eventLoopGroup;
    private final long periodMillis;

    public NetworkingImbalancePlugin(NodeEngineImpl nodeEngine) {
        this(nodeEngine.getProperties(), getThreadingModel(nodeEngine), nodeEngine.getLogger(NetworkingImbalancePlugin.class));
    }

    public NetworkingImbalancePlugin(HazelcastProperties properties, EventLoopGroup eventLoopGroup, ILogger logger) {
        super(logger);

        if (eventLoopGroup instanceof NioEventLoopGroup) {
            this.eventLoopGroup = (NioEventLoopGroup) eventLoopGroup;
        } else {
            this.eventLoopGroup = null;
        }
        this.periodMillis = this.eventLoopGroup == null ? 0 : properties.getMillis(PERIOD_SECONDS);
    }

    private static EventLoopGroup getThreadingModel(NodeEngineImpl nodeEngine) {
        ConnectionManager connectionManager = nodeEngine.getNode().getConnectionManager();
        if (!(connectionManager instanceof TcpIpConnectionManager)) {
            return null;
        }
        return ((TcpIpConnectionManager) connectionManager).getEventLoopGroup();
    }

    @Override
    public long getPeriodMillis() {
        return periodMillis;
    }

    @Override
    public void onStart() {
        logger.info("Plugin:active: period-millis:" + periodMillis);
    }

    @Override
    public void run(DiagnosticsLogWriter writer) {
        writer.startSection("NetworkingImbalance");

        writer.startSection("InputThreads");
        render(writer, eventLoopGroup.getInputThreads());
        writer.endSection();

        writer.startSection("OutputThreads");
        render(writer, eventLoopGroup.getOutputThreads());
        writer.endSection();

        writer.endSection();
    }

    private void render(DiagnosticsLogWriter writer, NioThread[] threads) {
        if (threads == null) {
            // this can become null due to stopping of the system
            return;
        }

        long totalPriorityFramesReceived = 0;
        long totalFramesReceived = 0;
        long totalBytesReceived = 0;
        long totalEvents = 0;
        long totalTaskCount = 0;
        long totalHandleCount = 0;

        for (NioThread thread : threads) {
            totalBytesReceived += thread.bytesTransceived();
            totalFramesReceived += thread.framesTransceived();
            totalPriorityFramesReceived += thread.priorityFramesTransceived();
            totalEvents += thread.eventCount();
            totalTaskCount += thread.completedTaskCount();
            totalHandleCount += thread.handleCount();
        }

        for (NioThread thread : threads) {
            writer.startSection(thread.getName());
            writer.writeKeyValueEntry("frames-percentage", toPercentage(thread.framesTransceived(), totalFramesReceived));
            writer.writeKeyValueEntry("frames", thread.framesTransceived());
            writer.writeKeyValueEntry("priority-frames-percentage",
                    toPercentage(thread.priorityFramesTransceived(), totalPriorityFramesReceived));
            writer.writeKeyValueEntry("priority-frames", thread.priorityFramesTransceived());
            writer.writeKeyValueEntry("bytes-percentage", toPercentage(thread.bytesTransceived(), totalBytesReceived));
            writer.writeKeyValueEntry("bytes", thread.bytesTransceived());
            writer.writeKeyValueEntry("events-percentage", toPercentage(thread.eventCount(), totalEvents));
            writer.writeKeyValueEntry("events", thread.eventCount());
            writer.writeKeyValueEntry("handle-count-percentage", toPercentage(thread.handleCount(), totalHandleCount));
            writer.writeKeyValueEntry("handle-count", thread.handleCount());
            writer.writeKeyValueEntry("tasks-percentage", toPercentage(thread.completedTaskCount(), totalTaskCount));
            writer.writeKeyValueEntry("tasks", thread.completedTaskCount());
            writer.endSection();
        }
    }

    private String toPercentage(long amount, long total) {
        double percentage = (HUNDRED * amount) / total;
        return String.format("%1$,.2f", percentage) + " %";
    }
}
