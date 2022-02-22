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

package com.hazelcast.spi.impl.eventservice.impl;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.internal.util.executor.StripedRunnable;

import java.util.UUID;

/**
 * An event processor responsible of fetching the registration and service responsible for the published event
 * and processing it. The processor is an instance of the {@link StripedRunnable} and events are processed on a
 * thread defined by the {@link #orderKey}. This means that all events with the same {@link #orderKey} will be ordered.
 * Any exception thrown when processing the event will be returned to the caller.
 */
public class EventProcessor implements StripedRunnable {

    private final EventServiceImpl eventService;
    private final int orderKey;
    private final EventEnvelope envelope;

    public EventProcessor(EventServiceImpl eventService, EventEnvelope envelope, int orderKey) {
        this.eventService = eventService;
        this.envelope = envelope;
        this.orderKey = orderKey;
    }

    @Override
    public void run() {
        process(envelope);
    }

    /**
     * Processes the event by dispatching it on the responsible {@link EventPublishingService}
     * together with the listener responsible for the event.
     *
     * @param envelope the event to be processed
     * @see EventPublishingService#dispatchEvent(Object, Object)
     */
    void process(EventEnvelope envelope) {
        Object event = getEvent(envelope);
        String serviceName = envelope.getServiceName();
        EventPublishingService<Object, Object> service = eventService.nodeEngine.getService(serviceName);
        Registration registration = getRegistration(envelope, serviceName);
        if (registration == null) {
            return;
        }
        service.dispatchEvent(event, registration.getListener());
    }


    /**
     * Returns the local registration responsible for the event and service or {@code null} if none exists,
     * the registration is not local or there is no listener in the registration.
     *
     * @param eventEnvelope the event for which we need the registration
     * @param serviceName   the service name
     * @return the listener registration or {@code null} if none exists, it is not local or there is no listener in
     * the registration
     */
    private Registration getRegistration(EventEnvelope eventEnvelope, String serviceName) {
        EventServiceSegment segment = eventService.getSegment(serviceName, false);
        if (segment == null) {
            if (eventService.nodeEngine.isRunning()) {
                eventService.logger.warning("No service registration found for " + serviceName);
            }
            return null;
        }

        UUID id = eventEnvelope.getEventId();
        Registration registration = (Registration) segment.getRegistrationIdMap().get(id);
        if (registration == null) {
            if (eventService.nodeEngine.isRunning()) {
                if (eventService.logger.isFinestEnabled()) {
                    eventService.logger.finest("No registration found for " + serviceName + " / " + id);
                }
            }
            return null;
        }

        if (!eventService.isLocal(registration)) {
            eventService.logger.severe("Invalid target for  " + registration);
            return null;
        }

        if (registration.getListener() == null) {
            eventService.logger.warning("Something seems wrong! Subscriber is local but listener instance is null! -> "
                    + registration);
            return null;
        }

        return registration;
    }

    /** Returns the deserialized event object contained in the {@code eventEnvelope} */
    private Object getEvent(EventEnvelope eventEnvelope) {
        Object event = eventEnvelope.getEvent();
        if (event instanceof Data) {
            event = eventService.nodeEngine.toObject(event);
        }
        return event;
    }

    @Override
    public int getKey() {
        return orderKey;
    }

    @Override
    public String toString() {
        return "EventProcessor{envelope=" + envelope + '}';
    }
}
