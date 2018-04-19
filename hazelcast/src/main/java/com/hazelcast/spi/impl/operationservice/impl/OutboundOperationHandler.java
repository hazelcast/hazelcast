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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;

import static com.hazelcast.nio.Packet.FLAG_URGENT;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Responsible for sending Operations to another member.
 */
public class OutboundOperationHandler {
    private final Address thisAddress;
    private final InternalSerializationService serializationService;
    private final Node node;

    public OutboundOperationHandler(Node node, Address thisAddress, InternalSerializationService serializationService) {
        this.node = node;
        this.thisAddress = thisAddress;
        this.serializationService = serializationService;
    }

    public boolean send(Operation op, Address target) {
        checkNotNull(target, "Target is required!");

        if (thisAddress.equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target + ", op: " + op);
        }

        Connection connection = node.getConnectionManager().getOrConnect(target);
        return send(op, connection);
    }

    public boolean send(Operation op, Connection connection) {
        byte[] bytes = serializationService.toBytes(op);
        int partitionId = op.getPartitionId();
        Packet packet = new Packet(bytes, partitionId).setPacketType(Packet.Type.OPERATION);

        if (op.isUrgent()) {
            packet.raiseFlags(FLAG_URGENT);
        }

        return node.getConnectionManager().transmit(packet, connection);
    }
}
