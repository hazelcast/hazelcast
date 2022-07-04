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

/**
 * Abstraction of Json Metadata Store.
 */
public interface JsonMetadataStore {

    @SuppressWarnings("AnonInnerLength")
    JsonMetadataStore NULL = new JsonMetadataStore() {

        @Override
        public JsonMetadata get(Data key) {
            return null;
        }

        @Override
        public void set(Data key, JsonMetadata metadata) {
            // no-op
        }

        @Override
        public void setKey(Data key, Object metadataKey) {
            // no-op
        }

        @Override
        public void setValue(Data key, Object metadataValue) {
            // no-op
        }

        @Override
        public void remove(Data key) {
            // no-op
        }

        @Override
        public void clear() {
            // no-op
        }

        @Override
        public void destroy() {
            // no-op
        }
    };

    /**
     * @param key the key in the store
     * @return the metadata associated with the key, {@code null} if there is no the key in the store
     */
    JsonMetadata get(Data key);

    /**
     * Puts the key/metadata pair into the store. Replaces the old metadata value
     * associated with the key if it exists.
     * @param key the key in the store
     * @param metadata the metadata
     */
    void set(Data key, JsonMetadata metadata);

    /**
     * Sets the new metadata key of the metadata associated with the key
     * @param key the key
     * @param metadataKey the matadata key
     */
    void setKey(Data key, Object metadataKey);

    /**
     * Sets the new metadata value of the metadata associated with the key
     * @param key the key
     * @param metadataValue the metadata value
     */
    void setValue(Data key, Object metadataValue);

    /**
     * Removes the key/metadata pair from the store
     * @param key the key
     */
    void remove(Data key);

    /**
     * Clears the store removing all key/metadata pairs.
     */
    void clear();

    /**
     * Destroys the data structures associated with the store.
     */
    void destroy();
}
