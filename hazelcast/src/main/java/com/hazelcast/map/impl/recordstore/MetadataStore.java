/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.impl.Metadata;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MetadataStore {

    private final ConcurrentMap<Data, Metadata> store;

    public MetadataStore() {
        this.store = new ConcurrentHashMap<>();
    }

    public Metadata get(Data key) {
        return store.get(key);
    }

    public void set(Data key, Metadata metadata) {
        store.put(key, metadata);
    }

    public void remove(Data key) {
        store.remove(key);
    }

    public void clear() {
        store.clear();
    }
}
