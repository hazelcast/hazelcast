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

package com.hazelcast.spi.impl.eventservice;

import com.hazelcast.nio.Packet;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;

/**
 * The InternalEventService is an {@link EventService} interface that adds additional capabilities
 * we don't want to expose to the end user. So they are purely meant to be used internally.
 */
public interface InternalEventService extends EventService {

    /**
     * Handles an event-packet.
     *
     * @param packet the event packet to handle.
     */
    void handleEvent(Packet packet);

    /**
     * Closes an EventRegistration.
     *
     * If the EventRegistration has any closeable resource, e.g. a listener, than this listener is closed.
     *
     * @param eventRegistration the EventRegistration to close.
     */
    void close(EventRegistration eventRegistration);
}
