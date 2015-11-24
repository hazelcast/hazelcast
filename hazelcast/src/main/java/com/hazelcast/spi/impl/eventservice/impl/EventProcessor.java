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

package com.hazelcast.spi.impl.eventservice.impl;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.util.executor.StripedRunnable;

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

    private Registration getRegistration(EventEnvelope eventEnvelope, String serviceName) {
        EventServiceSegment segment = eventService.getSegment(serviceName, false);
        if (segment == null) {
            if (eventService.nodeEngine.isRunning()) {
                eventService.logger.warning("No service registration found for " + serviceName);
            }
            return null;
        }

        String id = eventEnvelope.getEventId();
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
