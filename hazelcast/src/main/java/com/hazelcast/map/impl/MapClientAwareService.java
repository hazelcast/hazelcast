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

package com.hazelcast.map.impl;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.spi.ClientAwareService;

/**
 * Defines {@link ClientAwareService} behavior of map service.
 * This service is used to clean unneeded resources.
 *
 * @see ClientAwareService
 */
class MapClientAwareService implements ClientAwareService {

    private final MapServiceContext mapServiceContext;

    public MapClientAwareService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public void clientDisconnected(String clientUuid) {
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        PublisherContext publisherContext = queryCacheContext.getPublisherContext();
        publisherContext.handleDisconnectedSubscriber(clientUuid);
    }
}
