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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.querycache.subscriber.operation.DestroyQueryCacheOperation;
import com.hazelcast.map.impl.querycache.subscriber.operation.SetReadCursorOperation;

/**
 * {@code SubscriberContextSupport} implementation for node side.
 *
 * @see SubscriberContextSupport
 */
public class NodeSubscriberContextSupport implements SubscriberContextSupport {

    private final InternalSerializationService serializationService;

    public NodeSubscriberContextSupport(InternalSerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public Object createRecoveryOperation(String mapName, String cacheId, long sequence, int partitionId) {
        return new SetReadCursorOperation(mapName, cacheId, sequence, partitionId);
    }

    @Override
    public Boolean resolveResponseForRecoveryOperation(Object response) {
        return (Boolean) serializationService.toObject(response);
    }

    @Override
    public Object createDestroyQueryCacheOperation(String mapName, String cacheId) {
        return new DestroyQueryCacheOperation(mapName, cacheId);
    }
}
