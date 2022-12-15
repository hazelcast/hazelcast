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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.JobListener;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;

import java.util.Collection;

public class JobService implements EventPublishingService<JobEvent, JobListener> {
    public static final String SERVICE_NAME = "hz:impl:jobService";

    private final EventService eventService;

    public JobService(NodeEngine nodeEngine) {
        eventService = nodeEngine.getEventService();
    }

    @Override
    public void dispatchEvent(JobEvent event, JobListener listener) {
        listener.jobStatusChanged(event.getJobId(), event.getOldStatus(), event.getNewStatus(),
                event.getDescription(), event.isUserRequested());
    }

    public void publishEvent(long jobId, JobStatus oldStatus, JobStatus newStatus,
                             String description, boolean userRequested) {
        Collection<EventRegistration> registrations = eventService.getRegistrations(
                SERVICE_NAME, String.valueOf(jobId));
        if (!registrations.isEmpty()) {
            eventService.publishEvent(SERVICE_NAME, registrations, new JobEvent(
                    jobId, oldStatus, newStatus, description, userRequested), (int) jobId);
        }
    }
}
