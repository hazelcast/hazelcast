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

package com.hazelcast.wan.impl;

import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.internal.util.Preconditions;

import java.util.Collection;

/**
 * The result of adding WAN configuration.
 *
 * @see WanReplicationService#addWanReplicationConfig(WanReplicationConfig)
 */
public class AddWanConfigResult {

    private final Collection<String> addedPublisherIds;
    private final Collection<String> ignoredPublisherIds;

    public AddWanConfigResult(Collection<String> addedPublisherIds,
                              Collection<String> ignoredPublisherIds) {
        Preconditions.checkNotNull(addedPublisherIds, "Added publisher IDs must not be null");
        Preconditions.checkNotNull(ignoredPublisherIds, "Ignored publisher IDs must not be null");
        this.addedPublisherIds = addedPublisherIds;
        this.ignoredPublisherIds = ignoredPublisherIds;
    }


    /**
     * Returns the IDs for the WAN publishers which were added to the
     * configuration.
     */
    public Collection<String> getAddedPublisherIds() {
        return addedPublisherIds;
    }

    /**
     * Returns the IDs for the WAN publishers which were ignored and not added
     * to the configuration.
     */
    public Collection<String> getIgnoredPublisherIds() {
        return ignoredPublisherIds;
    }
}
