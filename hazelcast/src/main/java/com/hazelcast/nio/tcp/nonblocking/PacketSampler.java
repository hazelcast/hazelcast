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

package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.OutboundFrame;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

import static java.util.Collections.synchronizedSet;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The PacketSampler is a debug aid that periodically scans the Connection.writeQueues and samples the content.
 * This helps to figure out what is happening when writeQueues are very large. The PacketSampler is disabled
 * by default.
 */
public class PacketSampler extends Thread {
    // the delay between taking samples. Sampling has quite a lot of overhead, so doing it too frequent, will change
    // the behavior of the system.
    private static final int INTERVAL_SECONDS = Integer.getInteger("hazelcast.io.packetSampler.intervalSeconds", 0);
    // the minimum number of packets in the write queue. If there are less packets than the threshold, the connection
    // isn't sampled.
    private static final int THRESHOLD = Integer.getInteger("hazelcast.io.packetSampler.threshold", 10000);
    // the number of samples taken. The more samples, the more precise the information, but the more overhead (since it
    // involves deserialization).
    private static final int SAMPLE_COUNT = Integer.getInteger("hazelcast.io.packetSampler.sampleCount", 1000);

    private volatile boolean stop;
    private final ILogger logger;
    private final IOService ioService;
    private final StringBuilder sb = new StringBuilder();
    private final Map<String, Integer> occurrenceMap = new HashMap<String, Integer>();
    private final ArrayList<OutboundFrame> packets = new ArrayList<OutboundFrame>();
    private final Random random = new Random();
    private final Set<TcpIpConnection> connections = synchronizedSet(new HashSet<TcpIpConnection>());

    public PacketSampler(
            HazelcastThreadGroup hazelcastThreadGroup,
            IOService ioService) {
        super(hazelcastThreadGroup.getInternalThreadGroup(),
                hazelcastThreadGroup.getThreadNamePrefix("packetSamplerThread"));

        this.ioService = ioService;
        this.logger = ioService.getLogger(PacketSampler.class.getName());

        if (INTERVAL_SECONDS > 0) {
            logger.info("Enabling PacketSampler, interval-seconds:" + INTERVAL_SECONDS
                    + " threshold:" + THRESHOLD
                    + " sampleCount:" + SAMPLE_COUNT);
        }
    }

    public static boolean isPacketSamplerEnabled() {
        return INTERVAL_SECONDS > 0;
    }

    public void onConnectionAdded(TcpIpConnection connection) {
        connections.add(connection);
    }

    public void onConnectionRemoved(TcpIpConnection connection) {
        connections.remove(connection);
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                Thread.sleep(SECONDS.toMillis(INTERVAL_SECONDS));
            } catch (InterruptedException e) {
                if (stop) {
                    return;
                }
            }

            scan();
        }
    }

    private void scan() {
        for (TcpIpConnection connection : connections) {
            scan(connection, false);
            scan(connection, true);
        }
    }

    public void scan(TcpIpConnection connection, boolean priority) {
        clear();

        NonBlockingSocketWriter writer = (NonBlockingSocketWriter) connection.getSocketWriter();
        Queue<OutboundFrame> q = priority ? writer.urgentWriteQueue : writer.writeQueue;

        if (!sample(q)) {
            return;
        }

        sb.append("\n\t").append(connection).append('\n');
        if (priority) {
            sb.append("\turgent-packets:").append(packets.size()).append('\n');
        } else {
            sb.append("\tpackets:").append(packets.size()).append('\n');
        }
        sb.append("\tsample-count:").append(occurrenceMap.size()).append('\n');
        sb.append("\tsamples:\n");

        for (Map.Entry<String, Integer> entry : occurrenceMap.entrySet()) {
            sb.append('\t').append('\t').append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
        }

        logger.info(sb.toString());
    }

    private void clear() {
        sb.setLength(0);
        occurrenceMap.clear();
        packets.clear();
    }

    private boolean sample(Queue<OutboundFrame> q) {
        for (OutboundFrame frame : q) {
            packets.add(frame);
        }

        if (packets.size() < THRESHOLD) {
            return false;
        }

        int sampleCount = Math.min(SAMPLE_COUNT, packets.size());

        for (int k = 0; k < sampleCount; k++) {
            OutboundFrame packet = packets.get(random.nextInt(packets.size()));
            String key = toKey(packet);

            if (key != null) {
                Integer value = occurrenceMap.get(key);
                if (value == null) {
                    value = 0;
                }
                value++;
                occurrenceMap.put(key, value);
            }
        }

        return true;
    }

    private String toKey(OutboundFrame packet) {
        if (packet instanceof Packet) {
            try {
                Object result = ioService.toObject((Packet) packet);
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

    public void shutdown() {
        stop = true;
        interrupt();
    }
}
