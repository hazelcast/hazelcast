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

package com.hazelcast.internal.monitors;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.OutboundFrame;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingSocketWriter;
import com.hazelcast.nio.tcp.spinning.SpinningSocketWriter;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ItemCounter;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

import static java.lang.Math.min;
import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The OverloadedConnectionsPlugin checks all the connections and samples the content of the packet
 * queues if the size is above a certain threshold. This is very useful to figure out when huge
 * amount of memory is consumed due to pending packets.
 *
 * Currently the sampling has a lot of overhead since the value needs to be deserialized. That is why this
 * plugin is disabled by default.
 */
public class OverloadedConnectionsPlugin extends PerformanceMonitorPlugin {

    /**
     * The period in seconds the PerformanceMonitor OverloadedConnectionPlugin runs.
     *
     * With the OverloadedConnectionsPlugin one can see what is going on inside a connection with a huge
     * number of pending packets. It makes use of sampling to give some impression of the content.
     *
     * This plugin can be very expensive to use and should only be used as a debugging aid; should not be
     * used in production due to the fact that packets could be deserialized.
     *
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty PERIOD_SECONDS
            = new HazelcastProperty("hazelcast.performance.monitor.overloaded.connections.period.seconds", 0, SECONDS);

    /**
     * The minimum number of packets in the connection before it is considered to be overloaded.
     */
    public static final HazelcastProperty THRESHOLD
            = new HazelcastProperty("hazelcast.performance.monitor.overloaded.connections.threshold", 10000);

    /**
     * The number of samples to take from a single overloaded connection. Increasing the number of packages gives
     * more accuracy of the content, but it will come at greater price.
     */
    public static final HazelcastProperty SAMPLES
            = new HazelcastProperty("hazelcast.performance.monitor.overloaded.connections.samples", 1000);

    private static final Queue<OutboundFrame> EMPTY_QUEUE = new LinkedList<OutboundFrame>();

    private final SerializationService serializationService;
    private final ILogger logger;
    private final ItemCounter<String> occurrenceMap = new ItemCounter<String>();
    private final ArrayList<OutboundFrame> packets = new ArrayList<OutboundFrame>();
    private final Random random = new Random();
    private final NumberFormat defaultFormat = NumberFormat.getPercentInstance();
    private final NodeEngineImpl nodeEngine;
    private final long periodMillis;
    private final int threshold;
    private final int samples;

    public OverloadedConnectionsPlugin(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.serializationService = nodeEngine.getSerializationService();
        this.logger = nodeEngine.getLogger(OverloadedConnectionsPlugin.class);
        this.defaultFormat.setMinimumFractionDigits(3);

        HazelcastProperties props = nodeEngine.getProperties();
        this.periodMillis = props.getMillis(PERIOD_SECONDS);
        this.threshold = props.getInteger(THRESHOLD);
        this.samples = props.getInteger(SAMPLES);
    }

    @Override
    public long getPeriodMillis() {
        return periodMillis;
    }

    @Override
    public void onStart() {
        logger.info("Plugin:active, period-millis:" + periodMillis + " threshold:" + threshold + " samples:" + samples);
    }

    @Override
    public void run(PerformanceLogWriter writer) {
        writer.startSection("OverloadedConnections");

        Set<TcpIpConnection> connections = getTcpIpConnections();
        for (TcpIpConnection connection : connections) {
            clear();
            scan(writer, connection, false);

            clear();
            scan(writer, connection, true);
        }

        writer.endSection();
    }

    private Set<TcpIpConnection> getTcpIpConnections() {
        ConnectionManager connectionManager = nodeEngine.getNode().getConnectionManager();
        if (connectionManager instanceof TcpIpConnectionManager) {
            return ((TcpIpConnectionManager) connectionManager).getActiveConnections();
        } else {
            return emptySet();
        }
    }

    private void scan(PerformanceLogWriter writer, TcpIpConnection connection, boolean priority) {
        Queue<OutboundFrame> q = getOutboundQueue(connection, priority);

        int sampleCount = sample(q);
        if (sampleCount < 0) {
            return;
        }

        render(writer, connection, priority, sampleCount);
    }

    private Queue<OutboundFrame> getOutboundQueue(TcpIpConnection connection, boolean priority) {
        if (connection.getSocketWriter() instanceof NonBlockingSocketWriter) {
            NonBlockingSocketWriter writer = (NonBlockingSocketWriter) connection.getSocketWriter();
            return priority ? writer.urgentWriteQueue : writer.writeQueue;
        } else if (connection.getSocketWriter() instanceof SpinningSocketWriter) {
            SpinningSocketWriter writer = (SpinningSocketWriter) connection.getSocketWriter();
            return priority ? writer.urgentWriteQueue : writer.writeQueue;
        } else {
            return EMPTY_QUEUE;
        }
    }

    private void render(PerformanceLogWriter writer, TcpIpConnection connection, boolean priority, int sampleCount) {
        writer.startSection(connection.toString());

        writer.writeKeyValueEntry(priority ? "urgentPacketCount" : "packetCount", packets.size());
        writer.writeKeyValueEntry("sampleCount", sampleCount);
        renderSamples(writer, sampleCount);

        writer.endSection();
    }

    private void renderSamples(PerformanceLogWriter writer, int sampleCount) {
        writer.startSection("samples");

        for (String key : occurrenceMap.keySet()) {
            long value = occurrenceMap.get(key);
            if (value == 0) {
                continue;
            }

            double percentage = (1d * value) / sampleCount;
            writer.writeEntry(key + " sampleCount=" + value + " " + defaultFormat.format(percentage));
        }
        writer.endSection();
    }

    private void clear() {
        occurrenceMap.reset();
        packets.clear();
    }

    /**
     * Samples the queue.
     *
     * @param q the queue to sample.
     * @return the number of samples. If there were not sufficient samples, -1 is returned.
     */
    private int sample(Queue<OutboundFrame> q) {
        for (OutboundFrame frame : q) {
            packets.add(frame);
        }

        if (packets.size() < threshold) {
            return -1;
        }

        int sampleCount = min(samples, packets.size());
        int actualSampleCount = 0;
        for (int k = 0; k < sampleCount; k++) {
            OutboundFrame packet = packets.get(random.nextInt(packets.size()));
            String key = toKey(packet);

            if (key != null) {
                actualSampleCount++;
                occurrenceMap.add(key, 1);
            }
        }

        return actualSampleCount;
    }

    String toKey(OutboundFrame packet) {
        if (packet instanceof Packet) {
            try {
                Object result = serializationService.toObject(packet);
                if (result == null) {
                    return "null";
                } else if (result instanceof Backup) {
                    Backup backup = (Backup) result;
                    return Backup.class.getName() + "#" + backup.getBackupOp().getClass().getName();
                } else {
                    return result.getClass().getName();
                }
            } catch (Exception ignore) {
                logger.severe(ignore);
                return null;
            }
        } else {
            return packet.getClass().getName();
        }
    }
}
