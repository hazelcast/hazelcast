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

package com.hazelcast.internal.compatibility.wan;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Compatibility (3.x) WAN event class used to transmit the actual WAN event object
 */
public class CompatibilityWanReplicationEvent implements IdentifiedDataSerializable {

    private String serviceName;
    private CompatibilityReplicationEventObject eventObject;

    public CompatibilityWanReplicationEvent() {
    }

    /**
     * Returns the service name for this event object.
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Sets the service name for this event object.
     */
    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    /**
     * Gets the event object.
     */
    public CompatibilityReplicationEventObject getEventObject() {
        return eventObject;
    }

    /**
     * Sets the event object.
     */
    public void setEventObject(CompatibilityReplicationEventObject eventObject) {
        this.eventObject = eventObject;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException(getClass().getName() + " should not be serialized!");
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        serviceName = in.readUTF();
        eventObject = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return CompatibilityOSWanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CompatibilityOSWanDataSerializerHook.WAN_REPLICATION_EVENT;
    }
}
