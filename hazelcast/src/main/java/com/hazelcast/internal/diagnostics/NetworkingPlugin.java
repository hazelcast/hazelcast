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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.internal.networking.IOThreadingModel;
import com.hazelcast.internal.networking.nonblocking.NonBlockingIOThread;
import com.hazelcast.internal.networking.nonblocking.NonBlockingIOThreadingModel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import static com.hazelcast.internal.diagnostics.Diagnostics.PREFIX;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The {@link NetworkingPlugin} is an experimental plugin meant for detecting imbalance in the io system. This plugin will
 * probably mostly be used for internal purposes to get a better understanding of imbalances. Normally imbalances are taken
 * care of by the IOBalancer; but we need to make sure it makes the right choice.
 *
 * This plugin can be used on server and client side.
 */
public class NetworkingPlugin extends DiagnosticsPlugin {

    /**
     * The period in seconds this plugin runs.
     *
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty PERIOD_SECONDS = new HazelcastProperty(PREFIX + ".networking.seconds", 0, SECONDS);

    private static final double HUNDRED = 100d;

    private final NonBlockingIOThreadingModel ioThreadingModel;
    private final long periodMillis;

    public NetworkingPlugin(NodeEngineImpl nodeEngine) {
        this(nodeEngine.getProperties(), getThreadingModel(nodeEngine), nodeEngine.getLogger(NetworkingPlugin.class));
    }

    public NetworkingPlugin(HazelcastProperties properties, IOThreadingModel threadingModel, ILogger logger) {
        super(logger);

        if (threadingModel instanceof NonBlockingIOThreadingModel) {
            this.ioThreadingModel = (NonBlockingIOThreadingModel) threadingModel;
        } else {
            this.ioThreadingModel = null;
        }
        this.periodMillis = ioThreadingModel == null ? 0 : properties.getMillis(PERIOD_SECONDS);
    }

    private static IOThreadingModel getThreadingModel(NodeEngineImpl nodeEngine) {
        ConnectionManager connectionManager = nodeEngine.getNode().getConnectionManager();
        if (!(connectionManager instanceof TcpIpConnectionManager)) {
            return null;
        }
        return ((TcpIpConnectionManager) connectionManager).getIoThreadingModel();
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
        writer.startSection("Networking");

        writer.startSection("InputThreads");
        render(writer, ioThreadingModel.getInputThreads());
        writer.endSection();

        writer.startSection("OutputThreads");
        render(writer, ioThreadingModel.getOutputThreads());
        writer.endSection();

        writer.endSection();
    }

    private void render(DiagnosticsLogWriter writer, NonBlockingIOThread[] threads) {
        if (threads == null) {
            // this can become null due to stopping of the system.
            return;
        }

        long totalPriorityFramesReceived = 0;
        long totalFramesReceived = 0;
        long totalBytesReceived = 0;
        long totalEvents = 0;
        long totalTaskCount = 0;
        long totalHandleCount = 0;

        for (NonBlockingIOThread thread : threads) {
            totalBytesReceived += thread.bytesTransceived();
            totalFramesReceived += thread.framesTransceived();
            totalPriorityFramesReceived += thread.priorityFramesTransceived();
            totalEvents += thread.eventCount();
            totalTaskCount += thread.completedTaskCount();
            totalHandleCount += thread.handleCount();
        }

        for (NonBlockingIOThread thread : threads) {
            writer.startSection(thread.getName());
            writer.writeKeyValueEntry("frames", toPercentage(thread.framesTransceived(), totalFramesReceived));
            writer.writeKeyValueEntry("priority-frames",
                    toPercentage(thread.priorityFramesTransceived(), totalPriorityFramesReceived));
            writer.writeKeyValueEntry("bytes", toPercentage(thread.bytesTransceived(), totalBytesReceived));
            writer.writeKeyValueEntry("events", toPercentage(thread.eventCount(), totalEvents));
            writer.writeKeyValueEntry("handle-count", toPercentage(thread.handleCount(), totalHandleCount));
            writer.writeKeyValueEntry("tasks", toPercentage(thread.completedTaskCount(), totalTaskCount));
            writer.endSection();
        }
    }

    private String toPercentage(long amount, long total) {
        double percentage = (HUNDRED * amount) / total;
        return String.format("%1$,.2f", percentage) + " %";
    }
}
