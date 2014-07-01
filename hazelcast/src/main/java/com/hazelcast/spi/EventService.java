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

public interface EventService {

    int getEventThreadCount();

    int getEventQueueCapacity();

    int getEventQueueSize();

    EventRegistration registerLocalListener(String serviceName, String topic, Object listener);

    EventRegistration registerLocalListener(String serviceName, String topic, EventFilter filter, Object listener);

    EventRegistration registerListener(String serviceName, String topic, Object listener);

    EventRegistration registerListener(String serviceName, String topic, EventFilter filter, Object listener);

    boolean deregisterListener(String serviceName, String topic, Object id);

    void deregisterAllListeners(String serviceName, String topic);

    Collection<EventRegistration> getRegistrations(String serviceName, String topic);

    boolean hasEventRegistration(String topic);

    EventRegistration[] getRegistrationsAsArray(String serviceName, String topic);

    void publishEvent(String serviceName, EventRegistration registration, Object event, int orderKey);

    void publishEvent(String serviceName, Collection<EventRegistration> registrations, Object event, int orderKey);
}
