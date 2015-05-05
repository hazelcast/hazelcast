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

package com.hazelcast.spi.impl.eventservice.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.SpiDataSerializerHook;

import java.io.IOException;

public final class EventPacket implements IdentifiedDataSerializable {

    private String id;
    private String serviceName;
    private Object event;

    public EventPacket() {
    }

    EventPacket(String id, String serviceName, Object event) {
        this.event = event;
        this.id = id;
        this.serviceName = serviceName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public Object getEvent() {
        return event;
    }

    public String getEventId() {
        return id;
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SpiDataSerializerHook.EVENT_PACKET;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(serviceName);
        boolean isBinary = event instanceof Data;
        out.writeBoolean(isBinary);
        if (isBinary) {
            out.writeData((Data) event);
        } else {
            out.writeObject(event);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readUTF();
        serviceName = in.readUTF();
        boolean isBinary = in.readBoolean();
        if (isBinary) {
            event = in.readData();
        } else {
            event = in.readObject();
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EventPacket{");
        sb.append("id='").append(id).append('\'');
        sb.append(", serviceName='").append(serviceName).append('\'');
        sb.append(", event=").append(event);
        sb.append('}');
        return sb.toString();
    }
}
