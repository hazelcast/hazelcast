/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.map.MapStoreAdapter;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;

public class TemporaryBlockerMapStore extends MapStoreAdapter<String, String> {

    private final int blockStoreOperationSeconds;
    private final AtomicInteger storeOperationCount = new AtomicInteger(0);

    public TemporaryBlockerMapStore(int blockStoreOperationSeconds) {
        this.blockStoreOperationSeconds = blockStoreOperationSeconds;
    }

    @Override
    public void store(String key, String value) {
        storeOperationCount.incrementAndGet();
    }

    @Override
    public void storeAll(final Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            store(entry.getKey(), entry.getValue());
        }
        sleepSeconds(blockStoreOperationSeconds);
    }

    public int getStoreOperationCount() {
        return storeOperationCount.get();
    }
}
