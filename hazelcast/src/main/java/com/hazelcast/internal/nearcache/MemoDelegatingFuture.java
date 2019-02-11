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


import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.executor.DelegatingFuture;

public abstract class MemoDelegatingFuture<V, U> extends DelegatingFuture<V> {

    private volatile V value;

    MemoDelegatingFuture(InternalCompletableFuture future,
                         SerializationService ss) {
        super(future, ss);
    }

    public abstract Object getOrWaitValue(Object key);

    public abstract U getOrWaitUserResponse();

    protected abstract void onResponseReceived(V v);

    @Override
    protected final V resolve(Object object) {
        if (value == null) {
            synchronized (this) {
                if (value == null) {
                    V v = (V) object;
                    onResponseReceived(v);
                    value = v;
                    return v;
                }
            }
        }
        return value;
    }

    protected <T> T toObject(Object obj) {
        return serializationService.toObject(obj);
    }

    protected Data toData(Object obj) {
        return serializationService.toData(obj);
    }
}
