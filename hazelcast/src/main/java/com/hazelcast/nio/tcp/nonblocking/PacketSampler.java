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
import com.hazelcast.util.MutableInteger;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.getInteger;
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
    private static final int INTERVAL_SECONDS = getInteger("hazelcast.io.packetSampler.intervalSeconds", 0);
    // the minimum number of packets in the write queue. If there are less packets than the threshold, the connection
    // isn't sampled.
    private static final int THRESHOLD = getInteger("hazelcast.io.packetSampler.threshold", 10000);
    // the number of samples taken. The more samples, the more precise the information, but the more overhead (since it
    // involves deserialization).
    private static final int SAMPLE_COUNT = getInteger("hazelcast.io.packetSampler.sampleCount", 1000);
    // if the output should be put on a single line, or spread over multiple lines
    private static final boolean ONE_LINER = parseBoolean(System.getProperty("hazelcast.io.packetSampler.oneLiner", "false"));
    private static final double HUNDRED = 100d;

    private volatile boolean stop;
    private final ILogger logger;
    private final IOService ioService;
    private final StringBuilder sb = new StringBuilder();
    private final Map<String, MutableInteger> occurrenceMap = new HashMap<String, MutableInteger>();
    private final ArrayList<OutboundFrame> packets = new ArrayList<OutboundFrame>();
    private final Random random = new Random();
    private final Set<TcpIpConnection> connections = synchronizedSet(new HashSet<TcpIpConnection>());
    private final NumberFormat defaultFormat = NumberFormat.getPercentInstance();

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

        int sampleCount = sample(q);
        if (sampleCount < 0) {
            return;
        }

        if (ONE_LINER) {
            samplesToOneLineString(connection, priority, sampleCount);
        } else {
            samplesToString(connection, priority, sampleCount);
        }

        logger.info(sb.toString());
    }

    private void samplesToString(TcpIpConnection connection, boolean priority, int sampleCount) {
        sb.append("\n\t").append(connection).append('\n');
        if (priority) {
            sb.append("\turgentPackets:").append(packets.size()).append('\n');
        } else {
            sb.append("\tpackets:").append(packets.size()).append('\n');
        }
        sb.append("\ttotalSampleCount:").append(sampleCount).append('\n');
        sb.append("\tsamples:\n");

        defaultFormat.setMinimumFractionDigits(3);
        for (Map.Entry<String, MutableInteger> entry : occurrenceMap.entrySet()) {
            String key = entry.getKey();
            int value = entry.getValue().value;
            double percentage = (1d * value) / sampleCount;

            sb.append('\t').append('\t').append(key)
                    .append(" sampleCount:").append(value)
                    .append(" ").append(defaultFormat.format(percentage)).append('\n');
        }
    }

    private void samplesToOneLineString(TcpIpConnection connection, boolean priority, int sampleCount) {
        sb.append(connection).append("(");
        if (priority) {
            sb.append("urgentPackets=").append(packets.size()).append(',');
        } else {
            sb.append("packets=").append(packets.size()).append(',');
        }
        sb.append("totalSampleCount=").append(sampleCount).append(',');
        sb.append("samples=[");

        boolean first = true;
        for (Map.Entry<String, MutableInteger> entry : occurrenceMap.entrySet()) {
            String key = entry.getKey();
            int value = entry.getValue().value;
            double percentage = (HUNDRED * value) / sampleCount;

            if (first) {
                first = false;
            } else {
                sb.append(',');
            }

            sb.append(key).append("[")
                    .append("sampleCount=").append(value)
                    .append(",percentage=").append(percentage)
                    .append(']');
        }
        sb.append(']');
    }


    private void clear() {
        sb.setLength(0);
        occurrenceMap.clear();
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

        if (packets.size() < THRESHOLD) {
            return -1;
        }

        int sampleCount = Math.min(SAMPLE_COUNT, packets.size());
        int actualSampleCount = 0;
        for (int k = 0; k < sampleCount; k++) {
            OutboundFrame packet = packets.get(random.nextInt(packets.size()));
            String key = toKey(packet);

            if (key != null) {
                actualSampleCount++;
                MutableInteger counter = occurrenceMap.get(key);
                if (counter == null) {
                    counter = new MutableInteger();
                    occurrenceMap.put(key, counter);
                }
                counter.value++;
            }
        }

        return actualSampleCount;
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
