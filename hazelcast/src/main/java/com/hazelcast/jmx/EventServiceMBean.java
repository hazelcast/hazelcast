package com.hazelcast.jmx;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.EventService;

import java.util.Hashtable;

import static com.hazelcast.jmx.ManagementService.quote;

@ManagedDescription("HazelcastInstance.EventService")
public class EventServiceMBean extends HazelcastMBean<EventService> {

    public EventServiceMBean(HazelcastInstance hazelcastInstance, EventService eventService, ManagementService service) {
        super(eventService, service);

        Hashtable<String, String> properties = new Hashtable<String, String>(3);
        properties.put("type", quote("HazelcastInstance.EventService"));
        properties.put("name", quote(hazelcastInstance.getName()));
        properties.put("HazelcastInstance", quote(hazelcastInstance.getName()));

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

    @ManagedAnnotation("unprocessedEventCount")
    @ManagedDescription("The number of unprocessed events")
    public int getUnprocessedEventCount() {
        return managedObject.getUnprocessedEventCount();
    }
}

