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

package com.hazelcast.map.impl.event;

import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.BinaryInterface;

import java.io.IOException;

/**
 * Abstract event data.
 */
@BinaryInterface
abstract class AbstractEventData implements EventData {

    protected String source;
    protected String mapName;
    protected Address caller;
    protected int eventType;

    AbstractEventData() {
    }

    AbstractEventData(String source, String mapName, Address caller, int eventType) {
        this.source = source;
        this.mapName = mapName;
        this.caller = caller;
        this.eventType = eventType;
    }

    @Override
    public String getSource() {
        return source;
    }

    @Override
    public String getMapName() {
        return mapName;
    }

    @Override
    public Address getCaller() {
        return caller;
    }

    @Override
    public int getEventType() {
        return eventType;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(source);
        out.writeString(mapName);
        out.writeObject(caller);
        out.writeInt(eventType);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        source = in.readString();
        mapName = in.readString();
        caller = in.readObject();
        eventType = in.readInt();
    }

    @Override
    public String toString() {
        return "source='" + source + '\''
                + ", mapName='" + mapName + '\''
                + ", caller=" + caller
                + ", eventType=" + eventType;
    }
}
