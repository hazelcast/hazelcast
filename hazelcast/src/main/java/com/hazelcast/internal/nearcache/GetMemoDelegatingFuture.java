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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.nearcache.impl.store.NearCacheContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class GetMemoDelegatingFuture extends MemoDelegatingFuture<Object, Object> {

    private final FutureSupplier supplier;
    private final AtomicBoolean updateDone = new AtomicBoolean(false);

    private volatile Object userResponse;

    public GetMemoDelegatingFuture(boolean asyncUpdate,
                                   FutureSupplier futureSupplier,
                                   InternalCompletableFuture future,
                                   SerializationService ss) {
        super(future, ss);
        this.supplier = futureSupplier;
        if (asyncUpdate) {
            andThen(new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object response) {
                    updateNearCache(response);
                }

                @Override
                public void onFailure(Throwable t) {
                    Map<Object, NearCacheRecord> recordsByNcKey = supplier.getRecordsByNcKey();
                    for (Object ncKey : recordsByNcKey.keySet()) {
                        supplier.getContext().removeRecord(ncKey);
                    }
                }
            });
        }
    }

    @Override
    protected void onResponseReceived(Object response) {
        this.userResponse = toObject(response);

        updateNearCache(response);
    }

    private void updateNearCache(Object response) {
        if (!updateDone.compareAndSet(false, true)) {
            // to control only one near cache update from this future
            // since both async get and sync get can share this future
            return;
        }
        NearCacheContext context = supplier.getContext();
        Map<Object, NearCacheRecord> recordsByNcKey = supplier.getRecordsByNcKey();
        for (Map.Entry<Object, NearCacheRecord> entry : recordsByNcKey.entrySet()) {
            Object ncKey = entry.getKey();
            Data dataKey = supplier.getDataKey(ncKey);
            if (context.cachingAllowedFor(dataKey, response)) {
                context.updateRecordState(entry.getValue(), ncKey, response, userResponse, supplier);
            } else {
                context.removeRecord(ncKey);
            }
        }
    }

    @Override
    public Object getOrWaitValue(Object key) {
        join();
        return userResponse;
    }

    @Override
    public Object getOrWaitUserResponse() {
        join();
        return userResponse;
    }
}
