/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp;

/**
 * Represents the result of a {@link CPDataStructureManagementService#purgeCPMap} operation.
 *
 * @since 5.7
 */
public interface CPMapPurgeResponse {

    /**
     * Returns the number of entries removed during the purge operation.
     *
     * @return number of purged entries; always non-negative
     */
    int purgedCount();

    /**
     * Returns the timestamp of the oldest entry that remains after the purge.
     *
     * @return timestamp of the oldest remaining entry
     */
    long oldestRemainingEntryTimestamp();
}
