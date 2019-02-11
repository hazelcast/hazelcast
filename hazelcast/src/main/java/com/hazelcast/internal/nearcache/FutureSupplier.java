/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nearcache;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.nearcache.impl.MemoSupplier;
import com.hazelcast.internal.nearcache.impl.store.NearCacheContext;
import com.hazelcast.nio.serialization.Data;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class FutureSupplier extends MemoSupplier<ICompletableFuture> {

    private final ConcurrentMap<Object, Data> dataKeyByNcKey = new ConcurrentHashMap<Object, Data>();
    private final ConcurrentMap<Data, Object> ncKeyByDataKey = new ConcurrentHashMap<Data, Object>();
    private final ConcurrentMap<Object, NearCacheRecord> recordsByNcKey = new ConcurrentHashMap<Object, NearCacheRecord>();

    private NearCacheContext context;

    public ConcurrentMap<Object, Data> getDataKeyByNcKey() {
        return dataKeyByNcKey;
    }

    public void mapNcKeyToDataKey(Object ncKey, Data dataKey) {
        dataKeyByNcKey.put(ncKey, dataKey);
        ncKeyByDataKey.put(dataKey, ncKey);
    }

    public Data getDataKey(Object ncKey) {
        return dataKeyByNcKey.get(ncKey);
    }

    public Object getObjectKey(Data keyData) {
        return ncKeyByDataKey.get(keyData);
    }

    public Map<Object, NearCacheRecord> getRecordsByNcKey() {
        return recordsByNcKey;
    }

    public <K> void supplyValueFor(Object ncKey, NearCacheRecord newRecord) {
        recordsByNcKey.put(ncKey, newRecord);
    }

    public void setContext(NearCacheContext context) {
        this.context = context;
    }

    public NearCacheContext getContext() {
        return context;
    }
}
