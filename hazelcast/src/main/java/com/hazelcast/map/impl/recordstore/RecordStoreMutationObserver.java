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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;

import javax.annotation.Nonnull;

/**
 * Interface for observing {@link RecordStore} mutations
 *
 * @param <R> The type of the records in the observed {@link RecordStore}
 */
public interface RecordStoreMutationObserver<R extends Record> {
    /**
     * Called if the observed {@link RecordStore} is cleared
     */
    void onClear();

    /**
     * Called when a new record is added to the {@link RecordStore}
     *
     * @param key    The key of the record
     * @param record The record
     */
    void onPutRecord(@Nonnull Data key, @Nonnull R record);

    /**
     * Called when a new record is added to the {@link RecordStore} due
     * to replication
     *
     * @param key    The key of the record
     * @param record The record
     */
    void onReplicationPutRecord(@Nonnull Data key, @Nonnull R record);

    /**
     * Called when a new record is updated in the observed {@link RecordStore}
     *
     * @param key      The key of the record
     * @param record   The record
     * @param newValue The new value of the record
     */
    void onUpdateRecord(@Nonnull Data key, @Nonnull R record, Object newValue);

    /**
     * Called when a record is removed from the observed {@link RecordStore}
     *
     * @param key    The key of the record
     * @param record The record
     */
    void onRemoveRecord(@Nonnull Data key, R record);

    /**
     * Called when a record is evicted from the observed {@link RecordStore}
     *
     * @param key    The key of the record
     * @param record The record
     */
    void onEvictRecord(@Nonnull Data key, @Nonnull R record);

    /**
     * Called when a record is loaded into the observed {@link RecordStore}
     *
     * @param key    The key of the record
     * @param record The record
     */
    void onLoadRecord(@Nonnull Data key, @Nonnull R record);

    /**
     * Called when the observed {@link RecordStore} is being destroyed.
     * The observer should release all resources. The implementations of
     * this method should be idempotent.
     *
     * @param internal is only data bound to the {@link MapService} being destroyed
     */
    void onDestroy(boolean internal);

    /**
     * Called when the observed {@link RecordStore} is being reset.
     */
    void onReset();
}
