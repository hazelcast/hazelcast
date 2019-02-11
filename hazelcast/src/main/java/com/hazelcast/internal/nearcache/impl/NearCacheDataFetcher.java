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

package com.hazelcast.internal.nearcache.impl;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;

import java.util.Collection;

public interface NearCacheDataFetcher<V> {

    InternalCompletableFuture<Boolean> invokeContainsKeyOperation(Object key);

    InternalCompletableFuture invokeGetOperation(Data keyData, Data param, boolean asyncGet, long startTimeNanos);

    InternalCompletableFuture invokeGetAllOperation(Collection<Data> dataKeys, Data param, long startTimeNanos);

    V getWithoutNearCaching(Object key, Object param, long startTimeNanos);

    ICompletableFuture getAsyncWithoutNearCaching(Object key, Object param, long startTimeNanos);

    boolean isStatsEnabled();

    void interceptAfterGet(String name, V val);
}
