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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.impl.ICacheInternal;
import com.hazelcast.client.impl.spi.ClientContext;

public class ClientCachePartitionIterator<K, V> extends ClientClusterWideIterator<K, V> {

    public ClientCachePartitionIterator(ICacheInternal<K, V> cacheProxy, ClientContext context, int fetchSize,
                                        int partitionId, boolean prefetchValues) {
        super(cacheProxy, context, fetchSize, partitionId, prefetchValues);
    }

    @Override
    protected boolean advance() {
        if (lastTableIndex < 0) {
            lastTableIndex = Integer.MAX_VALUE;
            return false;
        }
        result = fetch();
        if (result != null && result.size() > 0) {
            index = 0;
            return true;
        }
        return false;
    }
}
