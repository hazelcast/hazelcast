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

public class MapInfo {

    private String name;
    private MapConfig mapConfig;
    private MapStore store;

    public MapInfo(String name, MapConfig mapConfig) {
        this.name = name;
        this.mapConfig = mapConfig;
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        if(mapStoreConfig != null && mapStoreConfig.getClassName() != null) {
            try {
                store = (MapStore) ClassLoaderUtil.newInstance(mapStoreConfig.getClassName());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public String getName() {
        return name;
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
