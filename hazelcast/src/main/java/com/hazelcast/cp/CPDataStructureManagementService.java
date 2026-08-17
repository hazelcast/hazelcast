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

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletionStage;

/**
 * Provides utility operations for managing CP data structures.
 *
 * @since 5.7
 */
public interface CPDataStructureManagementService {

    /**
     * Purges entries from the specified {@link CPMap} that are older than the given duration.
     *
     * @param mapName the name of the CPMap
     * @param age the age threshold; entries older than this duration are removed
     * @return a {@link CPMapPurgeResponse} containing the number of purged entries
     *         and the timestamp of the oldest remaining entry
     *
     * @throws NoSuchElementException if no CP group exists for the given {@code mapName}
     *  *         or if the CPMap with the given {@code mapName} has not been created
     * @throws UnsupportedOperationException if purge is not enabled for the given CPMap
     *         or if the cluster version is earlier than 5.7
     * @throws NullPointerException if {@code mapName} or {@code age} is {@code null}
     *
     * @see com.hazelcast.config.cp.CPMapConfig#setPurgeEnabled
     */
    CompletionStage<CPMapPurgeResponse> purgeCPMap(String mapName, Duration age);
}
