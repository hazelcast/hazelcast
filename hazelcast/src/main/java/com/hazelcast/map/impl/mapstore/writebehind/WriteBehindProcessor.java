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

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Write behind processors main contract.
 * Responsible for processing map store logic like retries, failures, listeners, batch size etc.
 *
 * @param <E> Type of object which is going to be processed by map store.
 */
public interface WriteBehindProcessor<E> {

    /**
     * Process store operations and returns failed operation per partition map.
     *
     * @param delayedEntries to be written to store.
     * @return failed store operations per partition.
     */
    Map<Integer, List<E>> process(List<E> delayedEntries);

    void callAfterStoreListeners(Collection<E> entries);

    void callBeforeStoreListeners(Collection<E> entries);

    void addStoreListener(StoreListener storeListener);

    /**
     * Flushes supplied {@link WriteBehindQueue} to map-store.
     *
     * @param queue supplied {@link WriteBehindQueue} for flush.
     */
    void flush(WriteBehindQueue queue);

    /**
     * Flushes a key directly to map store.
     *
     * @param key to be flushed.
     */
    void flush(E key);
}
