/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap;

import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.multimap.proxy.MultiMapProxy;
import com.hazelcast.multimap.proxy.ObjectMultiMapProxy;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ServiceProxy;

import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @ali 1/1/13
 */
public class MultiMapService implements ManagedService, RemoteService {

    private NodeEngine nodeEngine;

    public static final String MULTI_MAP_SERVICE_NAME = "hz:impl:multiMapService";

    private final ConcurrentMap<String, MultiMapProxy> proxies = new ConcurrentHashMap<String, MultiMapProxy>();

    private final MultiMapPartitionContainer[] partitionContainers;

    public MultiMapService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        partitionContainers = new MultiMapPartitionContainer[nodeEngine.getPartitionCount()];
    }

    public MultiMapContainer getMultiMap(int partitionId, String name){
        return partitionContainers[partitionId].getMultiMapContainer(name);
    }

    public MultiMapPartitionContainer getPartitionContainer(int partitionId){
        return partitionContainers[partitionId];
    }

    public MultiMapConfig getConfig(String name){
        return nodeEngine.getConfig().getMultiMapConfig(name);
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            partitionContainers[i] = new MultiMapPartitionContainer (this);
        }
    }

    public void destroy() {
    }

    public ServiceProxy getProxy(Object... params) {
        final String name = String.valueOf(params[0]);
        if (params.length > 1 && Boolean.TRUE.equals(params[1])) {
            //return new DataQueueProxy(name, this, nodeEngine);
        }
        final ObjectMultiMapProxy proxy = new ObjectMultiMapProxy(name, this, nodeEngine);
        final MultiMapProxy currentProxy = proxies.putIfAbsent(name, proxy);
        return currentProxy != null ? currentProxy : proxy;
    }

    public Collection<ServiceProxy> getProxies() {
        return new HashSet<ServiceProxy>(proxies.values());
    }
}
