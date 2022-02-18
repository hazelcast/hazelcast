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

package com.hazelcast.client.map.impl.querycache.subscriber;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ContinuousQueryDestroyCacheCodec;
import com.hazelcast.client.impl.protocol.codec.ContinuousQuerySetReadCursorCodec;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContextSupport;

/**
 * {@code SubscriberContextSupport} implementation for client side.
 *
 * @see SubscriberContextSupport
 */
public class ClientSubscriberContextSupport implements SubscriberContextSupport {

    public ClientSubscriberContextSupport() {
    }

    @Override
    public Object createRecoveryOperation(String mapName, String cacheId, long sequence, int partitionId) {
        return ContinuousQuerySetReadCursorCodec.encodeRequest(mapName, cacheId, sequence);
    }

    @Override
    public Boolean resolveResponseForRecoveryOperation(Object object) {
        return ContinuousQuerySetReadCursorCodec.decodeResponse((ClientMessage) object);
    }

    @Override
    public Object createDestroyQueryCacheOperation(String mapName, String cacheId) {
        return ContinuousQueryDestroyCacheCodec.encodeRequest(mapName, cacheId);
    }
}
