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

package com.hazelcast.map.impl;

import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.map.IMap;

/**
 * Adapter for all {@link IMap} listeners. This interface is considered to be used only
 * by {@link IMap} internals.
 * <p>
 * Also every {@link com.hazelcast.map.listener.MapListener} should be wrapped
 * in a {@link com.hazelcast.map.impl.ListenerAdapter} before {@link EventService} registration.
 */
public interface ListenerAdapter<T> {

    /**
     * Handle event.
     *
     * @param event type of event.
     */
    void onEvent(T event);
}
