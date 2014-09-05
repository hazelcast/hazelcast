/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

import static com.hazelcast.core.DistributedObjectEvent.EventType;

public final class DistributedObjectEventPacket implements DataSerializable {

    private EventType eventType;
    private String serviceName;
    private String name;

    public DistributedObjectEventPacket() {
    }

    public DistributedObjectEventPacket(EventType eventType, String serviceName, String name) {
        this.eventType = eventType;
        this.serviceName = serviceName;
        this.name = name;
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

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(eventType == EventType.CREATED);
        out.writeUTF(serviceName);
        // writing as object for backward-compatibility
        out.writeObject(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        eventType = in.readBoolean() ? EventType.CREATED : EventType.DESTROYED;
        serviceName = in.readUTF();
        name = in.readObject();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("DistributedObjectEvent");
        sb.append("{eventType=").append(eventType);
        sb.append(", serviceName='").append(serviceName).append('\'');
        sb.append(", name=").append(name);
        sb.append('}');
        return sb.toString();
    }
}
