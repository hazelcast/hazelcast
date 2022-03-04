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

package com.hazelcast.spi.impl;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.function.Consumer;

import static com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;
import static com.hazelcast.internal.nio.Packet.FLAG_OP_CONTROL;
import static com.hazelcast.internal.nio.Packet.FLAG_OP_RESPONSE;

/**
 * A {@link Consumer} that dispatches the {@link Packet} to the right service. For example, operations are sent to the
 * {@link OperationService}, events are sent to the {@link EventService} etc.
 */
public final class PacketDispatcher implements Consumer<Packet> {

    private final ILogger logger;
    private final Consumer<Packet> eventService;
    private final Consumer<Packet> operationExecutor;
    private final Consumer<Packet> jetServiceBackend;
    private final Consumer<Packet> responseHandler;
    private final Consumer<Packet> invocationMonitor;

    public PacketDispatcher(ILogger logger,
                            Consumer<Packet> operationExecutor,
                            Consumer<Packet> responseHandler,
                            Consumer<Packet> invocationMonitor,
                            Consumer<Packet> eventService,
                            Consumer<Packet> jetServiceBackend
    ) {
        this.logger = logger;
        this.responseHandler = responseHandler;
        this.eventService = eventService;
        this.invocationMonitor = invocationMonitor;
        this.operationExecutor = operationExecutor;
        this.jetServiceBackend = jetServiceBackend;
    }

    @Override
    public void accept(Packet packet) {
        try {
            switch (packet.getPacketType()) {
                case OPERATION:
                    if (packet.isFlagRaised(FLAG_OP_RESPONSE)) {
                        responseHandler.accept(packet);
                    } else if (packet.isFlagRaised(FLAG_OP_CONTROL)) {
                        invocationMonitor.accept(packet);
                    } else {
                        operationExecutor.accept(packet);
                    }
                    break;
                case EVENT:
                    eventService.accept(packet);
                    break;
                case SERVER_CONTROL:
                    ServerConnection connection = packet.getConn();
                    ServerConnectionManager connectionManager = connection.getConnectionManager();
                    connectionManager.accept(packet);
                    break;
                case JET:
                    jetServiceBackend.accept(packet);
                    break;
                default:
                    logger.severe("Header flags [" + Integer.toBinaryString(packet.getFlags())
                            + "] specify an undefined packet type " + packet.getPacketType().name());
            }
        } catch (Throwable t) {
            inspectOutOfMemoryError(t);
            logger.severe("Failed to process: " + packet, t);
        }
    }
}
