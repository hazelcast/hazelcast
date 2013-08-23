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

package com.hazelcast.core;

/**
 * DistributedObjectEvent is fired when a {@link DistributedObject}
 * is created or destroyed cluster-wide.
 *
 * @see DistributedObject
 * @see DistributedObjectListener
 */
public class DistributedObjectEvent {

    private EventType eventType;

    private String serviceName;

    private DistributedObject distributedObject;

    public DistributedObjectEvent(EventType eventType, String serviceName, DistributedObject distributedObject) {
        this.eventType = eventType;
        this.serviceName = serviceName;
        this.distributedObject = distributedObject;
    }

    /**
     * Returns service name of related DistributedObject
     *
     * @return service name of DistributedObject
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Returns type of this event; one of {@link EventType#CREATED} or {@link EventType#DESTROYED}
     *
     * @return eventType
     */
    public EventType getEventType() {
        return eventType;
    }

    /**
     * Returns identifier of related DistributedObject
     *
     * @return identifier of DistributedObject
     */
    public Object getObjectId() {
        return distributedObject.getId();
    }

    /**
     * Returns DistributedObject instance
     *
     * @return DistributedObject
     */
    public DistributedObject getDistributedObject() {
        return distributedObject;
    }

    public enum EventType {
        CREATED, DESTROYED
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DistributedObjectEvent{");
        sb.append("eventType=").append(eventType);
        sb.append(", serviceName='").append(serviceName).append('\'');
        sb.append(", distributedObject=").append(distributedObject);
        sb.append('}');
        return sb.toString();
    }
}
