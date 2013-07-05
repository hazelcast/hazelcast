/*
 * Copyright 2013 Hazelcast, Inc..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.cluster;

import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStoreFactory;
import java.util.Properties;

/**
 *
 * @author yueyulin
 */
public class BackupInstanceAwareStoreFactory implements MapStoreFactory<String, String> {

    public MapLoader<String, String> newMapStore(String mapName, Properties properties) {
        InstanceAwareStore store = new InstanceAwareStore();
        store.setIsBackup(true);
        String instanceId = properties.getProperty("instanceId");
        store.setInstanceId(instanceId);
        store.setMapName(mapName.substring(2));
        return store;
    }
    
}
