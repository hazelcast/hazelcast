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

package com.hazelcast.spi.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;
import static com.hazelcast.nio.Packet.FLAG_OP_CONTROL;
import static com.hazelcast.nio.Packet.FLAG_OP_RESPONSE;

/**
 * A {@link PacketHandler} that dispatches the {@link Packet} to the right service. So operations are send to the
 * {@link com.hazelcast.spi.OperationService}, events are send to the {@link com.hazelcast.spi.EventService} etc.
 */
public final class PacketDispatcher implements PacketHandler {

    private final ILogger logger;
    private final PacketHandler eventService;
    private final PacketHandler operationExecutor;
    private final PacketHandler jetService;
    private final PacketHandler connectionManager;
    private final PacketHandler responseHandler;
    private final PacketHandler invocationMonitor;

    public PacketDispatcher(ILogger logger,
                            PacketHandler operationExecutor,
                            PacketHandler responseHandler,
                            PacketHandler invocationMonitor,
                            PacketHandler eventService,
                            PacketHandler connectionManager,
                            PacketHandler jetService) {
        this.logger = logger;
        this.responseHandler = responseHandler;
        this.eventService = eventService;
        this.invocationMonitor = invocationMonitor;
        this.connectionManager = connectionManager;
        this.operationExecutor = operationExecutor;
        this.jetService = jetService;
    }

    @Override
    public void handle(Packet packet) throws Exception {
        try {
            switch (packet.getPacketType()) {
                case OPERATION:
                    if (packet.isFlagRaised(FLAG_OP_RESPONSE)) {
                        responseHandler.handle(packet);
                    } else if (packet.isFlagRaised(FLAG_OP_CONTROL)) {
                        invocationMonitor.handle(packet);
                    } else {
                        operationExecutor.handle(packet);
                    }
                    break;
                case EVENT:
                    eventService.handle(packet);
                    break;
                case BIND:
                    connectionManager.handle(packet);
                    break;
                case JET:
                    jetService.handle(packet);
                    break;
                default:
                    logger.severe("Header flags [" + Integer.toBinaryString(packet.getFlags())
                            + "] specify an undefined packet type " + packet.getPacketType().name());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            inspectOutOfMemoryError(t);
            logger.severe("Failed to process:" + packet, t);
        }
    }
}
