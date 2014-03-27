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

import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class PortableDistributedObjectEvent implements Portable {

    private DistributedObjectEvent.EventType eventType;

    private String name;

    private String serviceName;

    public PortableDistributedObjectEvent() {
    }

    public PortableDistributedObjectEvent(DistributedObjectEvent.EventType eventType, String name, String serviceName) {
        this.eventType = eventType;
        this.name = name;
        this.serviceName = serviceName;
    }

    public DistributedObjectEvent.EventType getEventType() {
        return eventType;
    }

    public String getName() {
        return name;
    }

    public String getServiceName() {
        return serviceName;
    }

    @Override
    public int getFactoryId() {
        return SpiPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return SpiPortableHook.DISTRIBUTED_OBJECT_EVENT;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeUTF("s", serviceName);
        writer.writeUTF("t", eventType.name());
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        serviceName = reader.readUTF("s");
        eventType = DistributedObjectEvent.EventType.valueOf(reader.readUTF("t"));
    }
}
