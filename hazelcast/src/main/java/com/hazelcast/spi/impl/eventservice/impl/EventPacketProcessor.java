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

public class EventPacketProcessor implements StripedRunnable {

    private final EventServiceImpl eventService;
    private final int orderKey;
    private final EventPacket eventPacket;

    public EventPacketProcessor(EventServiceImpl eventService, EventPacket packet, int orderKey) {
        this.eventService = eventService;
        this.eventPacket = packet;
        this.orderKey = orderKey;
    }

    @Override
    public void run() {
        process(eventPacket);
    }

    void process(EventPacket eventPacket) {
        Object eventObject = getEventObject(eventPacket);
        String serviceName = eventPacket.getServiceName();

        EventPublishingService<Object, Object> service = getPublishingService(serviceName);
        if (service == null) {
            return;
        }

        Registration registration = getRegistration(eventPacket, serviceName);
        if (registration == null) {
            return;
        }
        service.dispatchEvent(eventObject, registration.getListener());
    }

    private EventPublishingService<Object, Object> getPublishingService(String serviceName) {
        EventPublishingService<Object, Object> service = eventService.nodeEngine.getService(serviceName);
        if (service == null) {
            if (eventService.nodeEngine.isActive()) {
                eventService.logger.warning("There is no service named: " + serviceName);
            }
            return null;
        }
        return service;
    }

    private Registration getRegistration(EventPacket eventPacket, String serviceName) {
        EventServiceSegment segment = eventService.getSegment(serviceName, false);
        if (segment == null) {
            if (eventService.nodeEngine.isActive()) {
                eventService.logger.warning("No service registration found for " + serviceName);
            }
            return null;
        }

        String id = eventPacket.getEventId();
        Registration registration = (Registration) segment.getRegistrationIdMap().get(id);
        if (registration == null) {
            if (eventService.nodeEngine.isActive()) {
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

    private Object getEventObject(EventPacket eventPacket) {
        Object eventObject = eventPacket.getEvent();
        if (eventObject instanceof Data) {
            eventObject = eventService.nodeEngine.toObject(eventObject);
        }
        return eventObject;
    }

    @Override
    public int getKey() {
        return orderKey;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EventPacketProcessor{");
        sb.append("eventPacket=").append(eventPacket);
        sb.append('}');
        return sb.toString();
    }
}
