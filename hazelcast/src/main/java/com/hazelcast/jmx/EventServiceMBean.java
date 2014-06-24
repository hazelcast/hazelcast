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

package com.hazelcast.jmx;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.EventService;
import java.util.Hashtable;

import static com.hazelcast.jmx.ManagementService.quote;

/**
 * Management bean for {@link com.hazelcast.spi.EventService}
 */
@ManagedDescription("HazelcastInstance.EventService")
public class EventServiceMBean extends HazelcastMBean<EventService> {

    private static final int INITIAL_CAPACITY = 3;

    public EventServiceMBean(HazelcastInstance hazelcastInstance, EventService eventService, ManagementService service) {
        super(eventService, service);

        Hashtable<String, String> properties = new Hashtable<String, String>(INITIAL_CAPACITY);
        properties.put("type", quote("HazelcastInstance.EventService"));
        properties.put("name", quote(hazelcastInstance.getName()));
        properties.put("instance", quote(hazelcastInstance.getName()));

        setObjectName(properties);
    }

    @ManagedAnnotation("eventThreadCount")
    @ManagedDescription("The event thread count")
    public int getEventThreadCount() {
        return managedObject.getEventThreadCount();
    }

    @ManagedAnnotation("eventQueueCapacity")
    @ManagedDescription("The event queue capacity")
    public int getEventQueueCapacity() {
        return managedObject.getEventQueueCapacity();
    }

    @ManagedAnnotation("eventQueueSize")
    @ManagedDescription("The size of the event queue")
    public int getEventQueueSize() {
        return managedObject.getEventQueueSize();
    }
}

