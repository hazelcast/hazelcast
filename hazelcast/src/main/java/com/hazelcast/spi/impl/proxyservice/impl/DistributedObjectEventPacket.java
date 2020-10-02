/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.tenantcontrol.TenantControl;

public final class DistributedObjectEventPacket implements IdentifiedDataSerializable, Versioned {

    private EventType eventType;
    private String serviceName;
    private String name;
    private UUID source;
    private TenantControl tenantControl;

    public DistributedObjectEventPacket() {
    }

    public DistributedObjectEventPacket(EventType eventType, String serviceName, String name, UUID source,
            TenantControl tenantControl) {
        this.eventType = eventType;
        this.serviceName = serviceName;
        this.name = name;
        this.source = source;
        this.tenantControl = tenantControl;
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

    public TenantControl getTenantControl() {
        return tenantControl;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(eventType == EventType.CREATED);
        out.writeUTF(serviceName);
        out.writeUTF(name);
        UUIDSerializationUtil.writeUUID(out, source);
        if (out.getVersion().isGreaterOrEqual(Versions.V4_1)) {
            out.writeObject(tenantControl);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        eventType = in.readBoolean() ? EventType.CREATED : EventType.DESTROYED;
        serviceName = in.readUTF();
        name = in.readUTF();
        source = UUIDSerializationUtil.readUUID(in);
        tenantControl = in.getVersion()
                .isGreaterOrEqual(Versions.V4_1) ? in.readObject() : TenantControl.NOOP_TENANT_CONTROL;
    }

    @Override
    public String toString() {
        return "DistributedObjectEvent{"
                + "eventType=" + eventType
                + ", serviceName='" + serviceName + '\''
                + ", name=" + name
                + ", source=" + source
                + ", tenantControl=" + tenantControl
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
