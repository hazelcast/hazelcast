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

package com.hazelcast.spi.impl.packettransceiver.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.spi.impl.packettransceiver.PacketTransceiver;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;

/**
 * Default {@link com.hazelcast.spi.impl.packettransceiver.PacketTransceiver} implementation.
 */
public class PacketTransceiverImpl implements PacketTransceiver {

    private static final int RETRY_NUMBER = 5;
    private static final int DELAY_FACTOR = 100;

    private final Node node;
    private final ExecutionService executionService;
    private final ILogger logger;
    private final PacketHandler eventService;
    private final PacketHandler wanReplicationService;
    private final PacketHandler operationService;

    public PacketTransceiverImpl(Node node,
                                 ILogger logger,
                                 PacketHandler operationService,
                                 PacketHandler eventService,
                                 PacketHandler wanReplicationService,
                                 ExecutionService executionService) {
        this.node = node;
        this.executionService = executionService;
        this.operationService = operationService;
        this.eventService = eventService;
        this.wanReplicationService = wanReplicationService;
        this.logger = logger;
    }

    @Override
    public boolean transmit(Packet packet, Connection connection) {
        if (connection == null || !connection.isAlive()) {
            return false;
        }
        return connection.write(packet);
    }

    @Override
    public void receive(Packet packet) {
        try {
            if (packet.isHeaderSet(Packet.HEADER_OP)) {
                operationService.handle(packet);
            } else if (packet.isHeaderSet(Packet.HEADER_EVENT)) {
                eventService.handle(packet);
            } else if (packet.isHeaderSet(Packet.HEADER_WAN_REPLICATION)) {
                wanReplicationService.handle(packet);
            } else {
                logger.severe("Unknown packet type! Header: " + packet.getHeader());
            }
        } catch (Throwable t) {
            inspectOutputMemoryError(t);
            logger.severe("Failed to process packet:" + packet, t);
        }
    }

    /**
     * Retries sending packet maximum 5 times until connection to target becomes available.
     */
    @Override
    public boolean transmit(Packet packet, Address target) {
        return send(packet, target, null);
    }

    private boolean send(Packet packet, Address target, SendTask sendTask) {
        ConnectionManager connectionManager = node.getConnectionManager();
        Connection connection = connectionManager.getConnection(target);
        if (connection != null) {
            return transmit(packet, connection);
        }

        if (sendTask == null) {
            sendTask = new SendTask(packet, target);
        }

        final int retries = sendTask.retries;
        if (retries < RETRY_NUMBER && node.isActive()) {
            connectionManager.getOrConnect(target, true);
            // TODO: Caution: may break the order guarantee of the packets sent from the same thread!
            executionService.schedule(sendTask, (retries + 1) * DELAY_FACTOR, TimeUnit.MILLISECONDS);
            return true;
        }
        return false;
    }

    private final class SendTask implements Runnable {
        private final Packet packet;
        private final Address target;
        private volatile int retries;

        private SendTask(Packet packet, Address target) {
            this.packet = packet;
            this.target = target;
        }

        @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "single-writer, many-reader")
        @Override
        public void run() {
            retries++;
            if (logger.isFinestEnabled()) {
                logger.finest("Retrying[" + retries + "] packet send operation to: " + target);
            }
            send(packet, target, this);
        }
    }
}
