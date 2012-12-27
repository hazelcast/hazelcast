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

package com.hazelcast.queue;

import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.core.QueueStore;
import com.hazelcast.nio.Data;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @ali 12/14/12
 */
public class QueueStoreWrapper {

    private QueueStore store;

    private boolean enabled = false;

    private boolean async = false;

    private int memoryLimit = 5;

    private int bulkLoad = 3;

    public QueueStoreWrapper() {
    }

    public void setConfig(QueueStoreConfig storeConfig) {
        if (storeConfig == null) {
            return;
        }
        try {
            Class<?> storeClass = Class.forName(storeConfig.getClassName());
            store = (QueueStore) storeClass.newInstance();
            enabled = storeConfig.isEnabled();
            async = Boolean.parseBoolean(storeConfig.getProperty("async"));
            memoryLimit = Integer.parseInt(storeConfig.getProperty("memory-limit"));
            bulkLoad = Integer.parseInt(storeConfig.getProperty("bulk-load"));
            if (bulkLoad < 1){
                bulkLoad = 1;
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NumberFormatException e){
//            e.printStackTrace();
        }

    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isAsync() {
        return async;
    }

    public int getMemoryLimit() {
        return memoryLimit;
    }

    public int getBulkLoad() {
        return bulkLoad;
    }

    public void store(Long key, Data value) throws Exception {
        if (enabled) {
            store.store(key, new QueueStoreValue(value));
        }
    }

    public void storeAll(Map map) throws Exception {
        if (enabled) {
            store.storeAll(map);
        }
    }

    public void delete(Long key) throws Exception {
        if (enabled) {
            store.delete(key);
        }
    }

    public void deleteAll(Collection keys) throws Exception {
        if (enabled) {
            store.deleteAll(keys);
        }
    }

    public Data load(Long key) throws Exception {
        if (enabled) {
            QueueStoreValue val = (QueueStoreValue) store.load(key);
            if (val != null) {
                return val.getData();
            }
        }
        return null;
    }

    public Map<Long, QueueStoreValue> loadAll(Collection keys) throws Exception {
        if (enabled) {
            return store.loadAll(keys);
        }
        return null;
    }

    public Set<Long> loadAllKeys() throws Exception {
        if (enabled) {
            return store.loadAllKeys();
        }
        return new HashSet<Long>(0);
    }
}
