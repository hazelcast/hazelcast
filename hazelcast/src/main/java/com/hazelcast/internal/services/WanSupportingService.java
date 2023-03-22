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

package com.hazelcast.internal.services;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.wan.impl.InternalWanEvent;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

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

    /**
     * Processes a WAN sync batch asynchronously.
     *
     * @param batch           collection of events, which represents a batch
     * @param acknowledgeType determines should this method wait for the event to be processed fully
     *                        or should it return after the event has been dispatched to the
     *                        appropriate member
     * @return the CompletionStage to indicate processing of all entries in the batch
     */
    CompletionStage<Void> onSyncBatch(Collection<InternalWanEvent> batch, WanAcknowledgeType acknowledgeType);

    /**
     * Updates the related state when wan configuration is updated. For
     * example if a wan configuration is added and if there is an existing
     * maps referring to newly added wan configuration, some fields must be
     * updated in {@link com.hazelcast.map.impl.MapContainer}.
     */
    void onWanConfigChange();
}
