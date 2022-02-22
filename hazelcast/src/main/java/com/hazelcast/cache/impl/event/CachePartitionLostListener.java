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

package com.hazelcast.cache.impl.event;

import java.util.EventListener;

/**
 * Invoked when owner and all backups of a partition are lost for a specific cache.
 * @see CachePartitionLostEvent
 * @since 3.6
 */
public interface CachePartitionLostListener extends EventListener {

    /**
     * Invoked when owner and all backups of a partition are lost for a specific cache.
     *
     * @param event the event object that contains the cache name and lost partition ID.
     */
    void partitionLost(CachePartitionLostEvent event);

}
