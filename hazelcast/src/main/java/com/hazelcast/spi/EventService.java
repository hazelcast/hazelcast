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

package com.hazelcast.spi;

import java.util.Collection;

/**
 * Component responsible for handling events like topic events or map.listener events.
 */
public interface EventService {

    /**
     * Returns event thread count.
     *
     * @see com.hazelcast.instance.GroupProperties#PROP_EVENT_THREAD_COUNT
     * @return event thread count
     */
    int getEventThreadCount();

    /**
     * Returns queue capacity per event thread.
     *
     * @see com.hazelcast.instance.GroupProperties#PROP_EVENT_QUEUE_CAPACITY
     * @return queue capacity per event thread
     */
    int getEventQueueCapacity();

    /**
     * Returns current total event queue size.
     *
     * @return total event queue size
     */
    int getEventQueueSize();

    /**
     * Registers a local only listener.
     *
     * @param serviceName service name
     * @param topic topic name
     * @param listener listener instance
     * @return event registration
     */
    EventRegistration registerLocalListener(String serviceName, String topic, Object listener);

    /**
     * Registers a local only listener.
     *
     * @param serviceName service name
     * @param topic topic name
     * @param filter event filter
     * @param listener listener instance
     * @return event registration
     */
    EventRegistration registerLocalListener(String serviceName, String topic, EventFilter filter, Object listener);

    /**
     * Registers a listener on all cluster nodes.
     *
     * @param serviceName service name
     * @param topic topic name
     * @param listener listener instance
     * @return event registration
     */
    EventRegistration registerListener(String serviceName, String topic, Object listener);

    /**
     * Registers a listener on all cluster nodes.
     *
     * @param serviceName service name
     * @param topic topic name
     * @param filter event filter
     * @param listener listener instance
     * @return event registration
     */
    EventRegistration registerListener(String serviceName, String topic, EventFilter filter, Object listener);

    /**
     * Deregisters a listener with given registration id.
     *
     * @param serviceName service name
     * @param topic topic name
     * @param id registration id
     * @return true if listener is deregistered successfully, false otherwise
     *
     * @see EventRegistration#getId()
     * @see #registerListener(String, String, Object)
     * @see #registerLocalListener(String, String, Object)
     */
    boolean deregisterListener(String serviceName, String topic, Object id);

    /**
     * Deregisters all listeners belonging to the given service and topic.
     *
     * @param serviceName service name
     * @param topic topic name
     */
    void deregisterAllListeners(String serviceName, String topic);

    /**
     * Returns all registrations belonging to the given service and topic.
     *
     * @param serviceName service name
     * @param topic topic name
     * @return registrations
     */
    Collection<EventRegistration> getRegistrations(String serviceName, String topic);

    /**
     * Returns all registrations belonging to the given service and topic as an array.
     *
     * @param serviceName service name
     * @param topic topic name
     * @return registrations array
     */
    EventRegistration[] getRegistrationsAsArray(String serviceName, String topic);

    /**
     * Returns true if a listener is registered with specified service name and topic.
     *
     * @param serviceName service name
     * @param topic topic name
     * @return  true if a listener is registered with specified service name and topic,
     * false otherwise.
     */
    boolean hasEventRegistration(String serviceName, String topic);

    /**
     * Publishes an event for all event registrations belonging to specified service name and topic.
     *
     * @param serviceName service name
     * @param topic topic name
     * @param event event object
     * @param orderKey order key
     */
    void publishEvent(String serviceName, String topic, Object event, int orderKey);

    /**
     * Publishes an event for a specific event registration.
     *
     * @param serviceName service name
     * @param registration event registration
     * @param event event object
     * @param orderKey order key
     */
    void publishEvent(String serviceName, EventRegistration registration, Object event, int orderKey);

    /**
     * Publishes an event for multiple event registrations.
     *
     * @param serviceName service name
     * @param registrations multiple event registrations
     * @param event event object
     * @param orderKey order key
     */
    void publishEvent(String serviceName, Collection<EventRegistration> registrations, Object event, int orderKey);


    /**
     * Executes an event callback on a random event thread.
     * <p>
     * If {@code callback} is an instance of {@code StripedRunnable},
     * then {@code StripedRunnable.getKey()} will be used as order key
     * to pick event thread.
     * </p>
     *
     * @param callback callback to execute
     * @see com.hazelcast.util.executor.StripedRunnable
     */
    void executeEventCallback(Runnable callback);

}
