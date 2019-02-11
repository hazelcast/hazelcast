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

import com.hazelcast.internal.nearcache.impl.store.NearCacheContext;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.EMPTY_MAP;

public class GetAllMemoDelegatingFuture extends MemoDelegatingFuture<Object, Map> {

    private final FutureSupplier supplier;
    private final NearCacheContext context;

    private volatile Map userResponse;

    private volatile Map nearCacheResponse = EMPTY_MAP;

    public GetAllMemoDelegatingFuture(FutureSupplier supplier,
                                      InternalCompletableFuture future,
                                      SerializationService ss) {
        super(future, ss);
        this.supplier = supplier;
        this.context = supplier.getContext();
        Set<Object> keySet = supplier.getRecordsByNcKey().keySet();
        System.err.println("give: -" + keySet);
    }

    @Override
    protected void onResponseReceived(Object responseObject) {
        Map userResponse = new HashMap();

        if (responseObject instanceof Map) {
            Collection<Object> values = ((Map) responseObject).values();
            for (Object response : values) {
                if (response instanceof List) {
                    // for client side near cache
                    List<Map.Entry<Data, Data>> entries = (List<Map.Entry<Data, Data>>) response;
                    for (Map.Entry<Data, Data> entry : entries) {
                        handle(entry.getKey(), entry.getValue(), userResponse);
                    }
                } else {
                    // for server side near cache
                    MapEntries entries = toObject(response);
                    assert entries.size() == supplier.getRecordsByNcKey().size()
                            : entries.size() + " but " + supplier.getRecordsByNcKey().size();

                    for (int i = 0; i < entries.size(); i++) {
                        Data dataKey = entries.getKey(i);
                        Data dataValue = entries.getValue(i);

                        handle(dataKey, dataValue, userResponse);
                    }
                }
            }
        } else if (responseObject instanceof List) {
            // for client side near cache
            List<Map.Entry<Data, Data>> entries = (List<Map.Entry<Data, Data>>) responseObject;
            for (Map.Entry<Data, Data> entry : entries) {
                handle(entry.getKey(), entry.getValue(), userResponse);
            }
        }

        this.userResponse = userResponse;
    }

    private void handle(Data dataKey, Data dataValue, Map userResponse) {
        assert dataKey != null;
        assert dataValue != null;


        Object objectKey = context.serializeKeys() ? toObject(dataKey) : supplier.getObjectKey(dataKey);
        Object objectValue = toObject(dataValue);

        userResponse.put(objectKey, objectValue);

        updateNearCache(dataKey, objectKey, dataValue, objectValue);
    }

    // TODO: is null caching exists here for missing keys in response?
    private void updateNearCache(Data dataKey, Object objectKey, Data dataValue, Object objectValue) {
        Object ncKey = context.toNearCacheKey(dataKey, objectKey);

        if (context.cachingAllowedFor(dataKey, dataValue)) {
            Map<Object, NearCacheRecord> recordsByNcKey = supplier.getRecordsByNcKey();
            NearCacheRecord ncRecord = recordsByNcKey.get(ncKey);
            context.updateRecordState(ncRecord, ncKey, dataValue, objectValue, supplier);
        } else {
            context.removeRecord(ncKey);
        }

        if (context.serializeKeys()) {
            if (nearCacheResponse == EMPTY_MAP) {
                nearCacheResponse = new HashMap<Data, Object>();
            }
            nearCacheResponse.put(dataKey, objectValue);
        } else {
            nearCacheResponse = userResponse;
        }
    }

    @Override
    public Object getOrWaitValue(Object ncKey) {
        join();
        assert nearCacheResponse != null;
        return nearCacheResponse.get(ncKey);
    }

    @Override
    public Map getOrWaitUserResponse() {
        join();
        assert userResponse != null;
        return userResponse;
    }
}
