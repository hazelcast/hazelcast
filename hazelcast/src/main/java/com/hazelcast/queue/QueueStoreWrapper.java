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
import com.hazelcast.core.MapStore;
import com.hazelcast.core.QueueStore;
import com.hazelcast.nio.Data;

import java.util.*;

/**
 * @ali 12/14/12
 */
public class QueueStoreWrapper {

    QueueStore store;

    boolean enabled = false;

    boolean runOnBackup = false;

    public QueueStoreWrapper(){
    }

    public void setConfig(QueueStoreConfig storeConfig) {
        if (storeConfig == null) {
            System.out.println("config null");
            return;
        }
        enabled = storeConfig.isEnabled();
        String rob = storeConfig.getProperty("run-on-backup");
        runOnBackup = Boolean.parseBoolean(rob);
        try {
            Class<?> storeClass = Class.forName(storeConfig.getClassName());
            store = (QueueStore) storeClass.newInstance();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void store(Long key, Data value, boolean fromBackup) {
        if (check(fromBackup)) {
            store.store(key, new QueueStoreValue(value));
        }
    }

    public void storeAll(Map map, boolean fromBackup) {
        if (check(fromBackup)) {
            store.storeAll(map);
        }
    }

    public void delete(Long key, boolean fromBackup) {
        if (check(fromBackup)) {
            store.delete(key);
        }
    }

    public void deleteAll(Collection keys, boolean fromBackup) {
        if (check(fromBackup)) {
            store.deleteAll(keys);
        }
    }

    public Data load(Long key, boolean fromBackup) {
        if (check(fromBackup)) {
            QueueStoreValue val = (QueueStoreValue)store.load(key);
            if (val != null){
                return val.getData();
            }
            return null;
        }
        return null;
    }

    public Map<Long, QueueStoreValue> loadAll(Collection keys, boolean fromBackup) {
        if (check(fromBackup)) {
            return store.loadAll(keys);
        }
        return new HashMap<Long, QueueStoreValue>(0);
    }

    public Set<Long> loadAllKeys(boolean fromBackup) {
        if (check(fromBackup)) {
            return store.loadAllKeys();
        }
        return new HashSet<Long>(0);
    }

    private boolean check(boolean fromBackup) {
        if (enabled && store != null){
            if (runOnBackup || !fromBackup){
                return true;
            }
        }
        return false;
    }
}
