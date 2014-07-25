/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.client.spi.ProxyManager;
import com.hazelcast.client.spi.impl.ClientInvocationServiceImpl;

/**
 * date: 13/03/14
 * author: eminn
 */
public class StatisticsPusher implements Runnable {

    private final HazelcastClient hazelcastClient;
    private ClientInvocationServiceImpl invocationService;
    private final ProxyManager proxyManager;

    public StatisticsPusher(HazelcastClient hazelcastClient, ClientInvocationServiceImpl invocationService, ProxyManager proxyManager) {
        this.hazelcastClient = hazelcastClient;
        this.invocationService = invocationService;
        this.proxyManager = proxyManager;
    }

    @Override
    public void run() {
//        final Collection<? extends DistributedObject> objects = proxyManager.getDistributedObjects();
//        HashMap<String,CacheStatsImpl> statsMap = new HashMap<String,CacheStatsImpl>();
//        TimedClientState clientState = new TimedClientState();
//        for (DistributedObject object : objects) {
//            if (CacheService.SERVICE_NAME.equals(object.getServiceName())) {
//                final ClientCacheProxy cacheProxy = (ClientCacheProxy) object;
//                statsMap.put(cacheProxy.getName(), (CacheStatsImpl) cacheProxy.getStats());
//            }
//        }
//        Client localClient = hazelcastClient.getClientClusterService().getLocalClient();
//        if (localClient == null) {
//            return;
//        }
//        InetSocketAddress socketAddress = localClient.getSocketAddress();
//        if (socketAddress == null) {
//            return;
//        }
//        String clientAddress = socketAddress.getAddress().getHostAddress() + ":" + socketAddress.getPort();
//        clientState.setStatsMap(statsMap);
//        clientState.setClientAddress(clientAddress);
//        clientState.setTime(System.currentTimeMillis());
//        CachePushStatsRequest request = new CachePushStatsRequest(clientState);
//        try {
//            final Future future = hazelcastClient.getInvocationService().invokeOnRandomTarget(request);
//            future.get();
//        } catch (Exception e) {
//            throw ExceptionUtil.rethrow(e);
//        }
    }
}
