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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.internal.serialization.Data;

import static java.lang.Integer.getInteger;

/**
 * Evicts a {@link RecordStore}.
 * <p>
 * When the {@link RecordStore} needs to be evicted
 * according to {@link Evictor#checkEvictable}, {@link
 * Evictor} removes records from {@link RecordStore}.
 */
public interface Evictor {

    Evictor NULL_EVICTOR = new Evictor() {
        @Override
        public void evict(RecordStore recordStore, Data excludedKey) {

        }

        @Override
        public void forceEvictByPercentage(RecordStore recordStore, double evictionPercentage) {

        }

        @Override
        public boolean checkEvictable(RecordStore recordStore) {
            return false;
        }

        @Override
        public String toString() {
            return "Null Evictor implementation";
        }
    };

    String SYSTEM_PROPERTY_SAMPLE_COUNT = "hazelcast.map.eviction.sample.count";

    int DEFAULT_SAMPLE_COUNT = 15;

    int SAMPLE_COUNT = getInteger(SYSTEM_PROPERTY_SAMPLE_COUNT, DEFAULT_SAMPLE_COUNT);

    /**
     * Evict supplied record-store.
     *
     * @param recordStore the recordStore
     * @param excludedKey this key has lowest priority
     *                    to be selected for eviction and it is nullable.
     */
    void evict(RecordStore recordStore, Data excludedKey);

    /**
     * Evicts provided record store forcibly. This type
     * of eviction is used when regular eviction is not
     * enough to provide free space for newly added entries.
     *
     * @param recordStore        the record store
     * @param evictionPercentage percentage of the entries to evict from the record store
     */
    void forceEvictByPercentage(RecordStore recordStore, double evictionPercentage);

    /**
     * Check whether the supplied record-store needs eviction.
     *
     * @param recordStore the recordStore
     * @return {@code true} if eviction is required, {@code false} otherwise.
     */
    boolean checkEvictable(RecordStore recordStore);

}
