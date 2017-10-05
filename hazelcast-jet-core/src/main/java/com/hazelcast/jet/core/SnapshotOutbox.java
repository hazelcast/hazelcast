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

import javax.annotation.CheckReturnValue;

/**
 * An outbox which is used for offering items to processor's state snapshot.
 * <p>
 * The methods in this class may only be called from inside the
 * {@link Processor#saveToSnapshot()} method.
 */
public interface SnapshotOutbox {

    /**
     * Offers the specified key and value pair to the processor's snapshot storage.
     * <p>
     * During a snapshot restore the type of key offered determines which processors
     * receive the key and value pair. If the key is of type {@link BroadcastKey},
     * the entry will be restored to all processor instances.
     * Otherwise, the key will be distributed according to default partitioning and
     * only a single processor instance will receive the key.
     *
     * @return whether the outbox fully accepted the item
     */
    @CheckReturnValue
    boolean offer(Object key, Object value);
}
