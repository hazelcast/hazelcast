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

package com.hazelcast.map.listener;

import com.hazelcast.map.EventLostEvent;

/**
 * Invoked upon lost of event or events.
 *
 * That event lost can be both a real or a possible event lost situation.
 * In real event lost situation we can be sure that a sequence of events
 * are missing and a recovery can be tried but in possible event lost
 * situation we cannot be sure whether or not there is an event lost, for
 * example, after a node is killed suddenly, we cannot be sure that all
 * events on the killed node are sent and received by query caches.
 *
 * @since 3.5
 */
@FunctionalInterface
public interface EventLostListener extends MapListener {

    /**
     * Invoked upon lost of event or events.
     */
    void eventLost(EventLostEvent event);
}
