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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.proxy.NearCachedMapProxyImpl;
import com.hazelcast.map.merge.MergePolicyProvider;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;

import static com.hazelcast.internal.config.ConfigValidator.checkMapConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkNearCacheConfig;
import static com.hazelcast.internal.config.MergePolicyValidator.checkMergePolicySupportsInMemoryFormat;

/**
 * Defines remote service behavior of map service.
 *
 * @see MapService
 */
class MapRemoteService implements RemoteService {

    protected final MapServiceContext mapServiceContext;
    protected final NodeEngine nodeEngine;

    MapRemoteService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        Config config = nodeEngine.getConfig();
        MapConfig mapConfig = config.findMapConfig(name);
        MergePolicyProvider mergePolicyProvider = mapServiceContext.getMergePolicyProvider();
        checkMapConfig(mapConfig, mergePolicyProvider);

        Object mergePolicy = mergePolicyProvider.getMergePolicy(mapConfig.getMergePolicyConfig().getPolicy());
        checkMergePolicySupportsInMemoryFormat(name, mergePolicy, mapConfig.getInMemoryFormat(),
                nodeEngine.getClusterService().getClusterVersion(), true, nodeEngine.getLogger(getClass()));

        if (mapConfig.isNearCacheEnabled()) {
            checkNearCacheConfig(name, mapConfig.getNearCacheConfig(), config.getNativeMemoryConfig(), false);
            return new NearCachedMapProxyImpl(name, mapServiceContext.getService(), nodeEngine, mapConfig);
        } else {
            return new MapProxyImpl(name, mapServiceContext.getService(), nodeEngine, mapConfig);
        }
    }

    @Override
    public void destroyDistributedObject(String name) {
        mapServiceContext.destroyMap(name);
    }
}
