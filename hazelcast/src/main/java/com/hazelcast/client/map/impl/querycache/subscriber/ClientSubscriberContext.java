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

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.subscriber.AbstractSubscriberContext;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheEndToEndConstructor;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheRequest;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContextSupport;

/**
 * {@code SubscriberContext} implementation for client side.
 *
 * @see com.hazelcast.map.impl.querycache.subscriber.SubscriberContext
 */
public class ClientSubscriberContext extends AbstractSubscriberContext {

    private final ClientSubscriberContextSupport clientSubscriberContextSupport;

    public ClientSubscriberContext(QueryCacheContext context) {
        super(context);
        clientSubscriberContextSupport = new ClientSubscriberContextSupport();
    }

    @Override
    public SubscriberContextSupport getSubscriberContextSupport() {
        return clientSubscriberContextSupport;
    }

    @Override
    public QueryCacheEndToEndConstructor newEndToEndConstructor(QueryCacheRequest request) {
        return new ClientQueryCacheEndToEndConstructor(request);
    }
}
