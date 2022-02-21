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

package com.hazelcast.spi.impl.proxyservice.impl;

import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.SpiDataSerializerHook;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.core.DistributedObjectEvent.EventType;

public final class DistributedObjectEventPacket implements IdentifiedDataSerializable {

    private EventType eventType;
    private String serviceName;
    private String name;
    private UUID source;

    public DistributedObjectEventPacket() {
    }

    public DistributedObjectEventPacket(EventType eventType, String serviceName, String name, UUID source) {
        this.eventType = eventType;
        this.serviceName = serviceName;
        this.name = name;
        this.source = source;
    }

    public String getServiceName() {
        return serviceName;
    }

    public EventType getEventType() {
        return eventType;
    }

    public String getName() {
        return name;
    }

    public UUID getSource() {
        return source;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(eventType == EventType.CREATED);
        out.writeString(serviceName);
        out.writeString(name);
        UUIDSerializationUtil.writeUUID(out, source);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        eventType = in.readBoolean() ? EventType.CREATED : EventType.DESTROYED;
        serviceName = in.readString();
        name = in.readString();
        source = UUIDSerializationUtil.readUUID(in);
    }

    @Override
    public String toString() {
        return "DistributedObjectEvent{"
                + "eventType=" + eventType
                + ", serviceName='" + serviceName + '\''
                + ", name=" + name
                + ", source=" + source
                + '}';
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SpiDataSerializerHook.DISTRIBUTED_OBJECT_EVENT_PACKET;
    }
}
