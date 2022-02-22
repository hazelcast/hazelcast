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

import com.hazelcast.map.impl.mapstore.TestEntryStore;

import java.util.Map;
import java.util.concurrent.Semaphore;

public class TemporaryBlockerEntryStore<K, V> extends TestEntryStore<K, V> {

    Semaphore storePermit = new Semaphore(0);
    Semaphore storeAllPermit = new Semaphore(0);

    @Override
    public void store(K key, MetadataAwareValue<V> value) {
        try {
            storePermit.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        super.store(key, value);
    }

    @Override
    public void storeAll(Map<K, MetadataAwareValue<V>> map) {
        try {
            storeAllPermit.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        super.storeAll(map);
    }
}
