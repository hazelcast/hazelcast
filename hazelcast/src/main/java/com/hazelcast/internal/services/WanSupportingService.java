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

package com.hazelcast.internal.services;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.wan.impl.InternalWanEvent;

/**
 * An interface that can be implemented by internal services to give them the
 * ability to listen to WAN replication events.
 */
public interface WanSupportingService {

    /**
     * Processes a WAN replication event.
     *
     * @param event           the event
     * @param acknowledgeType determines should this method wait for the event to be processed fully
     *                        or should it return after the event has been dispatched to the
     *                        appropriate member
     */
    void onReplicationEvent(InternalWanEvent event, WanAcknowledgeType acknowledgeType);
}
