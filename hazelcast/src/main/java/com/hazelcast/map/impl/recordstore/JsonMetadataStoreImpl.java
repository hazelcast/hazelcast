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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.impl.JsonMetadata;
import com.hazelcast.query.impl.Metadata;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * On-heap Json Metadata Store. Uses CHM to store the key/metadata pairs.
 */
public class JsonMetadataStoreImpl implements JsonMetadataStore {

    // We use CHM since the map might be accessed from partition
    // and query threads concurrently
    private final ConcurrentMap<Data, JsonMetadata> store;

    public JsonMetadataStoreImpl() {
        this.store = new ConcurrentHashMap<>();
    }

    @Override
    public JsonMetadata get(Data key) {
        return store.get(key);
    }

    @Override
    public void set(Data key, JsonMetadata metadata) {
        store.put(key, metadata);
    }

    @Override
    public void setKey(Data key, Object metadataKey) {
        Metadata metadata = (Metadata) store.get(key);
        if (metadata == null) {
            if (metadataKey == null) {
                return;
            }

            store.put(key, new Metadata(metadataKey, null));
        } else {
            metadata.setKeyMetadata(metadataKey);

            if (metadata.getKeyMetadata() == null
                && metadata.getValueMetadata() == null) {
                store.remove(key);
            }
        }
    }

    @Override
    public void setValue(Data key, Object metadataValue) {
        Metadata metadata = (Metadata) store.get(key);
        if (metadata == null) {
            if (metadataValue == null) {
                return;
            }
            store.put(key, new Metadata(null, metadataValue));
        } else {
            metadata.setValueMetadata(metadataValue);

            if (metadata.getKeyMetadata() == null
                && metadata.getValueMetadata() == null) {
                store.remove(key);
            }
        }
    }


    @Override
    public void remove(Data key) {
        store.remove(key);
    }

    @Override
    public void clear() {
        store.clear();
    }

    @Override
    public void destroy() {
        // no-op
    }
}
