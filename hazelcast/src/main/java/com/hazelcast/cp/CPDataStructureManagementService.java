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
import java.util.concurrent.CompletableFuture;

/**
 * Utility methods for managing data structures.
 *
 * @since 5.7.0
 */
public interface CPDataStructureManagementService {

    /**
     * Purges entries from the given CPMap that are older than the specified duration.
     *
     * @param mapName          name of the CPMap
     * @param age age threshold for entries to be purged
     * @return a response object which holds the number of entries purged and the oldest remaining entry timestamp
     * @throws IllegalArgumentException      if no CPGroup exists for the given mapName
     * @throws UnsupportedOperationException if purge is not enabled for the given mapName
     *                                       or if the cluster version is earlier than 5.7
     * @throws NullPointerException          if mapName or age is {@code null}
     * @see com.hazelcast.config.cp.CPMapConfig#setPurgeEnabled
     */
    CompletableFuture<CPMapPurgeResponse> purgeCPMap(String mapName, Duration age);

}
