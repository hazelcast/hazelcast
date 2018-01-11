/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
 * A service for publishing events. For example a Topic that receives an message (the event)
 * and dispatches it to a listener.
 *
 * @param <E> the event type
 * @param <T> the event listener type
 */
public interface EventPublishingService<E, T> {

    /**
     * Notifies the service of a published event.
     *
     * @param event    the published event
     * @param listener the listener registered for this event
     */
    void dispatchEvent(E event, T listener);
}
