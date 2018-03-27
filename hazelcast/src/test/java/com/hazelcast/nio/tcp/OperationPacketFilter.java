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

package com.hazelcast.nio.tcp;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.Packet;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public abstract class OperationPacketFilter implements PacketFilter {
    protected final InternalSerializationService serializationService;

    protected OperationPacketFilter(InternalSerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public final Action filter(Packet packet, Address endpoint) {
        return packet.getPacketType() != Packet.Type.OPERATION ? Action.ALLOW : filterOperation(packet, endpoint);
    }

    private Action filterOperation(Packet packet, Address endpoint) {
        try {
            ObjectDataInput input = serializationService.createObjectDataInput(packet);
            byte header = input.readByte();
            boolean identified = (header & 1) != 0;
            if (identified) {
                boolean compressed = (header & 1 << 2) != 0;
                int factory = compressed ? input.readByte() : input.readInt();
                int type = compressed ? input.readByte() : input.readInt();
                return filterOperation(endpoint, factory, type);
            }
        } catch (IOException e) {
            throw new HazelcastException(e);
        }
        return Action.ALLOW;
    }

    protected abstract Action filterOperation(Address endpoint, int factory, int type);
}
