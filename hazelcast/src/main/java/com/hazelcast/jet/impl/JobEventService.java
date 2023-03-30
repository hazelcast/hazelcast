/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.JobStatusEvent;
import com.hazelcast.jet.JobStatusListener;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.eventservice.impl.Registration;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.Util.idToString;

public class JobEventService implements EventPublishingService<JobStatusEvent, JobStatusListener> {
    public static final String SERVICE_NAME = "hz:impl:jobEventService";

    private final EventServiceImpl eventService;
    private final Address address;

    public JobEventService(NodeEngine nodeEngine) {
        eventService = (EventServiceImpl) nodeEngine.getEventService();
        address = nodeEngine.getThisAddress();
    }

    @Override
    public void dispatchEvent(JobStatusEvent event, JobStatusListener listener) {
        listener.jobStatusChanged(event);
    }

    public void publishEvent(long jobId, JobStatus oldStatus, JobStatus newStatus,
                             String description, boolean userRequested) {
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, idToString(jobId));
        if (!registrations.isEmpty()) {
            JobStatusEvent event = new JobStatusEvent(jobId, oldStatus, newStatus, description, userRequested);
            eventService.publishEvent(SERVICE_NAME, registrations, event, (int) jobId);
        }
    }

    public UUID addEventListener(long jobId, JobStatusListener listener) {
        return eventService.registerListener(SERVICE_NAME, idToString(jobId), listener).getId();
    }

    public Registration prepareRegistration(long jobId, JobStatusListener listener, boolean localOnly) {
        UUID registrationId = UuidUtil.newUnsecureUUID();
        Registration registration = new Registration(registrationId, SERVICE_NAME, idToString(jobId),
                TrueEventFilter.INSTANCE, address, listener, localOnly);
        eventService.cacheListener(registration);
        return registration;
    }

    public EventRegistration handleAllRegistrations(long jobId, Registration registration) {
        return eventService.handleAllRegistrations(registration, (int) jobId);
    }

    public boolean removeEventListener(long jobId, UUID id) {
        return eventService.deregisterListener(SERVICE_NAME, idToString(jobId), id);
    }

    public CompletableFuture<Boolean> removeEventListenerAsync(long jobId, UUID id) {
        return eventService.deregisterListenerAsync(SERVICE_NAME, idToString(jobId), id);
    }

    public void removeAllEventListeners(long jobId) {
        eventService.deregisterAllListeners(SERVICE_NAME, idToString(jobId), (int) jobId);
    }
}
