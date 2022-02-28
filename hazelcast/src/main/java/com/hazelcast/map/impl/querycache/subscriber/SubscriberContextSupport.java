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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.map.QueryCache;

/**
 * Contains various helpers for {@code SubscriberContext}.
 */
public interface SubscriberContextSupport {

    /**
     * Creates recovery operation for event loss cases.
     *
     * @param mapName     map name
     * @param cacheId     ID of cache
     * @param sequence    sequence to be set
     * @param partitionId partitions ID of broken sequence
     * @return operation or request according to context
     * @see QueryCache#tryRecover()
     */
    Object createRecoveryOperation(String mapName, String cacheId, long sequence, int partitionId);

    /**
     * Resolves response of recoveryOperation.
     *
     * @param response clientMessage or data
     * @return resolved response
     */
    Boolean resolveResponseForRecoveryOperation(Object response);

    /**
     * Creates recovery operation for event loss cases.
     *
     * @param mapName map name
     * @param cacheId ID of cache
     * @return operation or request according to context
     * @see QueryCache#tryRecover()
     */
    Object createDestroyQueryCacheOperation(String mapName, String cacheId);
}
