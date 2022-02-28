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

import java.util.UUID;

/**
 * Base interface of WAN Synchronization statistics.
 */
public interface WanSyncStats {

    /**
     * Returns the UUID of the synchronization.
     *
     * @return the UUID of the synchronization
     */
    UUID getUuid();

    /**
     * Returns the duration of the synchronization in seconds.
     *
     * @return the duration of the synchronization in seconds
     */
    long getDurationSecs();

    /**
     * Returns the number of the partitions to synchronize.
     *
     * @return the number of partitions to synchronize
     */
    int getPartitionsToSync();

    /**
     * Returns the number of the partitions synchronized.
     *
     * @return the number of the partitions synchronized
     */
    int getPartitionsSynced();

    /**
     * Returns the number of the records synchronized.
     *
     * @return the number of records synchronized
     */
    int getRecordsSynced();
}
