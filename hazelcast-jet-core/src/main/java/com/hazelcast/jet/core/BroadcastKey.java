/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.hazelcast.jet.impl.execution.BroadcastKeyReference;

/**
 * Marker interface for a key in the snapshot state that indicates the
 * corresponding entry should be broadcast to all processors
 * when restoring the snapshot.
 *
 * @param <K> type of key
 */
public interface BroadcastKey<K> {

    /**
     * Returns the underlying key
     */
    K key();

    /**
     * Returns a given key as a broadcast key
     */
    static <K> BroadcastKey<K> broadcastKey(K key) {
        return new BroadcastKeyReference<>(key);
    }

}
