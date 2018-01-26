/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.Packet;
import com.hazelcast.util.executor.StripedRunnable;

/**
 * An extension of the {@link EventProcessor} which logs and swallows any exception while processing the event.
 * The {@link #orderKey} for this processor is equal to the packet partition ID. This means that when running
 * inside a {@link com.hazelcast.util.executor.StripedExecutor}, all events for the same partition ID will be ordered.
 *
 * @see EventServiceImpl#sendEvent(com.hazelcast.nio.Address, EventEnvelope, int)
 */
public class RemoteEventProcessor extends EventProcessor implements StripedRunnable {
    private EventServiceImpl eventService;
    private Packet packet;

    public RemoteEventProcessor(EventServiceImpl eventService, Packet packet) {
        super(eventService, null, packet.getPartitionId());
        this.eventService = eventService;
        this.packet = packet;
    }

    @Override
    public void run() {
        try {
            EventEnvelope eventEnvelope = (EventEnvelope) eventService.nodeEngine.toObject(packet);
            process(eventEnvelope);
        } catch (Exception e) {
            eventService.logger.warning("Error while logging processing event", e);
        }
    }
}
