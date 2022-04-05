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

package com.hazelcast.core;

import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.EventObject;
import java.util.UUID;

/**
 * DistributedObjectEvent is fired when a {@link DistributedObject}
 * is created or destroyed cluster-wide.
 *
 * @see DistributedObject
 * @see DistributedObjectListener
 */
@SuppressFBWarnings("SE_BAD_FIELD")
public class DistributedObjectEvent extends EventObject {
    private static final long serialVersionUID = -4532279189146831926L;
    protected DistributedObject distributedObject;

    private EventType eventType;
    private String serviceName;
    private String objectName;

    /**
     * Constructs a DistributedObject Event.
     *  @param eventType         The event type as an enum {@link EventType} integer.
     * @param serviceName       The service name of the DistributedObject.
     * @param objectName        The name of the DistributedObject.
     * @param distributedObject The DistributedObject for the event.
     * @param source            The UUID of the client or member which initialized create/destroy.
     */
    public DistributedObjectEvent(EventType eventType, String serviceName, String objectName, DistributedObject distributedObject,
                                  UUID source) {
        super(source);
        this.eventType = eventType;
        this.serviceName = serviceName;
        this.objectName = objectName;
        this.distributedObject = distributedObject;
    }

    /**
     * Returns the service name of related DistributedObject.
     *
     * @return service name of DistributedObject
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Returns the type of this event; one of {@link EventType#CREATED} or {@link EventType#DESTROYED}.
     *
     * @return The type of this event {@link EventType}.
     */
    public EventType getEventType() {
        return eventType;
    }

    /**
     * Returns the name of related DistributedObject.
     *
     * @return the name of DistributedObject
     * @see DistributedObject#getName()
     */
    public Object getObjectName() {
        return objectName;
    }

    /**
     * Returns the DistributedObject instance.
     *
     * @return the DistributedObject instance
     * @throws DistributedObjectDestroyedException if distributed object is destroyed.
     */
    public DistributedObject getDistributedObject() {
        if (EventType.DESTROYED.equals(eventType)) {
            throw new DistributedObjectDestroyedException(objectName + " destroyed!");
        }
        return distributedObject;
    }

    /**
     * Type of the DistributedObjectEvent.
     */
    public enum EventType {
        /**
         * Event if a DistributedObjectEvent is created.
         */
        CREATED,
        /**
         * Event if a DistributedObjectEvent is destroyed.
         */
        DESTROYED
    }

    @Override
    public String toString() {
        return "DistributedObjectEvent{" + "distributedObject=" + distributedObject + ", eventType=" + eventType
                + ", serviceName='" + serviceName + '\'' + ", objectName='" + objectName + '\'' + ", source=" + source + '}';
    }
}
