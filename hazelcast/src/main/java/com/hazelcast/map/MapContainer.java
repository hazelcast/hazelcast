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

package com.hazelcast.map;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.MapStore;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.query.impl.IndexService;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class MapContainer {

    private final String name;
    private final MapConfig mapConfig;
    private final MapStore store;
    // TODO: do we really need to store interceptors in 3 separate collections?
    // TODO: at first pahse you can remove the ability to removeInterceptor
    private final List<MapInterceptor> interceptors;
    private final Map<String, MapInterceptor> interceptorMap;
    private final Map<MapInterceptor, String> interceptorIdMap;
    private final IndexService indexService = new IndexService();
    private final boolean nearCacheEnabled;

    public MapContainer(String name, MapConfig mapConfig) {
        this.name = name;
        this.mapConfig = mapConfig;
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        if (mapStoreConfig != null && mapStoreConfig.getClassName() != null) {
            MapStore tmp;
            try {
                tmp = (MapStore) ClassLoaderUtil.newInstance(mapStoreConfig.getClassName());
            } catch (Exception e) {
                tmp = null;
                e.printStackTrace();
            }
            store = tmp;
        }
        else {
            store = null;
        }
        interceptors = new CopyOnWriteArrayList<MapInterceptor>();
        interceptorMap = new ConcurrentHashMap<String, MapInterceptor>();
        interceptorIdMap = new ConcurrentHashMap<MapInterceptor, String>();
        nearCacheEnabled = mapConfig.getNearCacheConfig() != null;
    }

    public IndexService getIndexService() {
        return indexService;
    }

    public String addInterceptor(MapInterceptor interceptor) {
        String id = "interceptor" + UUID.randomUUID();
        interceptorMap.put(id, interceptor);
        interceptorIdMap.put(interceptor, id);
        interceptors.add(interceptor);
        return id;
    }

    public void addInterceptor(MapInterceptor interceptor, String id) {
        interceptorMap.put(id, interceptor);
        interceptorIdMap.put(interceptor, id);
        interceptors.add(interceptor);
    }

    public List<MapInterceptor> getInterceptors() {
        return interceptors;
    }

    public String removeInterceptor(MapInterceptor interceptor) {
        String id = interceptorIdMap.remove(interceptor);
        interceptorMap.remove(id);
        interceptors.remove(interceptor);
        return id;
    }

    public void removeInterceptor(String id) {
        MapInterceptor interceptor = interceptorMap.remove(id);
        interceptorIdMap.remove(interceptor);
        interceptors.remove(interceptor);
    }

    public String getName() {
        return name;
    }

    public boolean isNearCacheEnabled() {
        return nearCacheEnabled;
    }

    public int getBackupCount() {
        return mapConfig.getBackupCount();
    }

    public long getWriteDelayMillis() {
        return mapConfig.getMapStoreConfig().getWriteDelaySeconds() * 1000;
    }

    public int getAsyncBackupCount() {
        return mapConfig.getAsyncBackupCount();
    }

    public MapConfig getMapConfig() {
        return mapConfig;
    }

    public MapStore getStore() {
        return store;
    }



}
