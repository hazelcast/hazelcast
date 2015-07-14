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

package com.hazelcast.spi;

/**
 * Contract point for event listeners to be notified by {@link com.hazelcast.spi.EventService}.
 *
 * @param <S> the type of the {@link com.hazelcast.spi.ManagedService}
 */
public interface NotifiableEventListener<S> {

    /**
     * Called when this listener registered to {@link com.hazelcast.spi.EventService}.
     *
     * @param service       the service instance that event belongs to
     * @param serviceName   name of the service that event belongs to
     * @param topic         name of the topic that event belongs to
     * @param registration  the {@link com.hazelcast.spi.EventRegistration} instance
     *                      that holds information about the registration
     */
    void onRegister(S service, String serviceName, String topic, EventRegistration registration);

    /**
     * Called when this listener deregistered from {@link com.hazelcast.spi.EventService}.
     *
     * @param service       the service instance that event belongs to
     * @param serviceName   name of the service that event belongs to
     * @param topic         name of the topic that event belongs to
     * @param registration  the {@link com.hazelcast.spi.EventRegistration} instance
     *                      that holds information about the registration
     */
    void onDeregister(S service, String serviceName, String topic, EventRegistration registration);

}
