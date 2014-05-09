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

package com.hazelcast.map.writebehind.store;

import com.hazelcast.map.MapStoreWrapper;
import com.hazelcast.map.writebehind.DelayedEntry;

import java.util.Map;
import java.util.Set;

/**
 * Handles map store write operations.
 */
class WriteStoreHandler extends AbstactStoreHandler<DelayedEntry> {

    protected WriteStoreHandler(MapStoreWrapper storeWrapper) {
        super(storeWrapper);
    }

    @Override
    public boolean processSingle(Object key, Object value) {
        return writeSingle(key, value);
    }

    @Override
    public boolean processBatch(Map map) {
        return writeBatch(map);
    }

    private boolean writeSingle(Object key, Object value) {
        //  delete operation if value is null.
        if (value == null) {
            return false;
        }
        mapStoreWrapper.store(key, value);
        return true;
    }

    private boolean writeBatch(Map entries) {
        final Set<Map.Entry> set = entries.entrySet();
        for (Map.Entry entry : set) {
            //  delete operation.
            if (entry.getValue() == null) {
                return false;
            }
        }
        mapStoreWrapper.storeAll(entries);
        return true;
    }
}
