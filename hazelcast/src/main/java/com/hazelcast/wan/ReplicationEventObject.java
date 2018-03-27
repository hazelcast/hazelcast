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

package com.hazelcast.wan;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.wan.impl.WanEventCounter;

/**
 * Interface for all WAN replication messages
 */
public interface ReplicationEventObject {
    /**
     * Increments the count for the related event in the {@code eventCounter}
     *
     * @param eventCounter the WAN event counter
     */
    void incrementEventCount(WanEventCounter eventCounter);

    /**
     * Returns the key for the entry on which the event occurred.
     */
    Data getKey();
}
