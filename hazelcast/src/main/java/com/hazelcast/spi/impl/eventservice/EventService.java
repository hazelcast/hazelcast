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

package com.hazelcast.spi.impl.eventservice;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.services.PostJoinAwareService;
import com.hazelcast.internal.services.PreJoinAwareService;
import com.hazelcast.spi.properties.ClusterProperty;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Component responsible for handling events like topic events or map.listener events. The events are divided into topics.
 */
public interface EventService extends Consumer<Packet>, PreJoinAwareService, PostJoinAwareService {

    /**
     * Returns the event thread count.
     *
     * @return the event thread count
     * @see ClusterProperty#EVENT_THREAD_COUNT
     */
    int getEventThreadCount();

    /**
     * Returns the queue capacity per event thread.
     *
     * @return the queue capacity per event thread
     * @see ClusterProperty#EVENT_QUEUE_CAPACITY
     */
    int getEventQueueCapacity();

    /**
     * Returns the current total event queue size.
     *
     * @return the current total event queue size
     */
    int getEventQueueSize();

    /**
     * Registers a local only listener.
     *
     * @param serviceName service name
     * @param topic       topic name
     * @param listener    listener instance
     * @return event registration
     * @throws IllegalArgumentException if the listener is {@code null}
     */
    EventRegistration registerLocalListener(@Nonnull String serviceName,
                                            @Nonnull String topic,
                                            @Nonnull Object listener);

    /**
     * Registers a local only listener with an event filter.
     *
     * @param serviceName service name
     * @param topic       topic name
     * @param filter      event filter
     * @param listener    listener instance
     * @return event registration
     * @throws IllegalArgumentException if the listener or filter is {@code null}
     */
    EventRegistration registerLocalListener(@Nonnull String serviceName,
                                            @Nonnull String topic,
                                            @Nonnull EventFilter filter,
                                            @Nonnull Object listener);

    /**
     * Registers a listener on all cluster nodes.
     *
     * @param serviceName service name
     * @param topic       topic name
     * @param listener    listener instance
     * @return event registration
     * @throws IllegalArgumentException if the listener is {@code null}
     */
    EventRegistration registerListener(@Nonnull String serviceName,
                                       @Nonnull String topic,
                                       @Nonnull Object listener);

    /**
     * Registers a listener on all cluster nodes.
     *
     * @param serviceName service name
     * @param topic       topic name
     * @param listener    listener instance
     * @return event registration future
     * @throws IllegalArgumentException if the listener is {@code null}
     */
    CompletableFuture<EventRegistration> registerListenerAsync(@Nonnull String serviceName,
                                                               @Nonnull String topic,
                                                               @Nonnull Object listener);

    /**
     * Registers a listener on all cluster nodes with an event filter.
     *
     * @param serviceName service name
     * @param topic       topic name
     * @param filter      event filter
     * @param listener    listener instance
     * @return event registration
     * @throws IllegalArgumentException if the listener or filter is {@code null}
     */
    EventRegistration registerListener(@Nonnull String serviceName,
                                       @Nonnull String topic,
                                       @Nonnull EventFilter filter,
                                       @Nonnull Object listener);

    /**
     * Registers a listener on all cluster nodes with an event filter.
     *
     * @param serviceName service name
     * @param topic       topic name
     * @param filter      event filter
     * @param listener    listener instance
     * @return event registration future
     * @throws IllegalArgumentException if the listener or filter is {@code null}
     */
    CompletableFuture<EventRegistration> registerListenerAsync(@Nonnull String serviceName,
                                       @Nonnull String topic,
                                       @Nonnull EventFilter filter,
                                       @Nonnull Object listener);

    /**
     * Deregisters a listener with the given registration ID.
     *
     * @param serviceName service name
     * @param topic       topic name
     * @param id          registration ID
     * @return true if listener is deregistered successfully, false otherwise
     * @see EventRegistration#getId()
     * @see #registerListener(String, String, Object)
     * @see #registerLocalListener(String, String, Object)
     */
    boolean deregisterListener(@Nonnull String serviceName,
                               @Nonnull String topic,
                               @Nonnull Object id);

    /**
     * Deregisters a listener with the given registration ID.
     *
     * @param serviceName service name
     * @param topic       topic name
     * @param id          registration ID
     * @return de-registration future. The future returns true if listener is deregistered successfully, false otherwise
     * @see EventRegistration#getId()
     * @see #registerListener(String, String, Object)
     * @see #registerListenerAsync(String, String, Object)
     * @see #registerLocalListener(String, String, Object)
     */
    CompletableFuture<Boolean> deregisterListenerAsync(@Nonnull String serviceName,
                               @Nonnull String topic,
                               @Nonnull Object id);

    /**
     * Deregisters all listeners belonging to the given service and topic.
     *
     * @param serviceName service name
     * @param topic       topic name
     */
    void deregisterAllListeners(@Nonnull String serviceName, @Nonnull String topic);

    /**
     * Returns all registrations belonging to the given service and topic.
     *
     * @param serviceName service name
     * @param topic       topic name
     * @return registrations
     */
    Collection<EventRegistration> getRegistrations(@Nonnull String serviceName, @Nonnull String topic);

    /**
     * Returns all registrations belonging to the given service and topic as an array.
     *
     * @param serviceName service name
     * @param topic       topic name
     * @return registrations array
     */
    EventRegistration[] getRegistrationsAsArray(@Nonnull String serviceName, @Nonnull String topic);

    /**
     * Returns true if a listener is registered with the specified service name and topic.
     *
     * @param serviceName service name
     * @param topic       topic name
     * @return true if a listener is registered with specified service name and topic,
     * false otherwise.
     */
    boolean hasEventRegistration(@Nonnull String serviceName, @Nonnull String topic);

    /**
     * Publishes an event for all event registrations belonging to the specified service name and topic.
     * All events with the same {@code orderKey} are ordered, otherwise the order is not preserved.
     * <p>
     * NOTE : The order may not be preserved in case the event needs to be republished (e.g. when the registration
     * is not on this node and the event has to be retransmitted)
     *
     * @param serviceName service name
     * @param topic       topic name
     * @param event       event object
     * @param orderKey    the order key for this event. All events with the same order key are ordered.
     */
    void publishEvent(String serviceName, String topic, Object event, int orderKey);

    /**
     * Publishes an event for a specific event registration.
     * All events with the same {@code orderKey} are ordered, otherwise the order is not preserved.
     * <p>
     * NOTE : The order may not be preserved in case the event needs to be republished (e.g. when the registration
     * is not on this node and the event has to be retransmitted)
     *
     * @param serviceName  service name
     * @param registration event registration
     * @param event        event object
     * @param orderKey     the order key for this event. All events with the same order key are ordered.
     */
    void publishEvent(String serviceName, EventRegistration registration, Object event, int orderKey);

    /**
     * Publishes an event for multiple event registrations.
     * All events with the same {@code orderKey} are ordered, otherwise the order is not preserved.
     * <p>
     * NOTE : The order may not be preserved in case the event needs to be republished (e.g. when the registration
     * is not on this node and the event has to be retransmitted)
     *
     * @param serviceName   service name
     * @param registrations multiple event registrations
     * @param event         event object
     * @param orderKey      the order key for this event. All events with the same order key are ordered.
     */
    void publishEvent(String serviceName, Collection<EventRegistration> registrations, Object event, int orderKey);

    /**
     * Publishes an event for multiple event registrations, excluding local ones.
     * All events with the same {@code orderKey} are ordered, otherwise the order is not preserved.
     * <p>
     * NOTE : The order may not be preserved in case the event needs to be republished (e.g. when the registration
     * is not on this node and the event has to be retransmitted)
     *
     * @param serviceName   service name
     * @param registrations multiple event registrations
     * @param event         event object
     * @param orderKey      the order key for this event. All events with the same order key are ordered.
     */
    void publishRemoteEvent(String serviceName,
                            Collection<EventRegistration> registrations,
                            Object event,
                            int orderKey);

    /**
     * Executes an event callback on a random event thread.
     * <p>
     * If {@code callback} is an instance of {@link com.hazelcast.internal.util.executor.StripedRunnable},
     * then {@link com.hazelcast.internal.util.executor.StripedRunnable#getKey()} will be used as order key
     * to pick event thread.
     * </p>
     *
     * @param callback the callback to execute on a random event thread
     * @see com.hazelcast.internal.util.executor.StripedRunnable
     */
    void executeEventCallback(@Nonnull Runnable callback);

    /**
     * Closes an EventRegistration.
     * <p>
     * If the EventRegistration has any closeable resource, e.g. a listener, than this listener is closed.
     *
     * @param eventRegistration the EventRegistration to close.
     */
    void close(EventRegistration eventRegistration);
}
