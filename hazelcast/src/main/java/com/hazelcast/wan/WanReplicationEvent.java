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

package com.hazelcast.wan;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Event class used to transmit the actual event object
 */
public class WanReplicationEvent
        implements DataSerializable {

    private String serviceName;
    private ReplicationEventObject eventObject;

    public WanReplicationEvent() {
    }

    public WanReplicationEvent(String serviceName, ReplicationEventObject eventObject) {
        this.serviceName = serviceName;
        this.eventObject = eventObject;
    }

    /**
     * Returns the service name for this event object.
     *
     * @return the service name for this event object.
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Sets the service name for this event object.
     *
     * @param serviceName the service name for this event object.
     */
    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    /**
     * Gets the event object.
     *
     * @return the event object.
     */
    public Object getEventObject() {
        return eventObject;
    }

    /**
     * Sets the event object.
     *
     * @param eventObject the event object.
     */
    public void setEventObject(ReplicationEventObject eventObject) {
        this.eventObject = eventObject;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(serviceName);
        out.writeObject(eventObject);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        serviceName = in.readUTF();
        eventObject = in.readObject();
    }
}
