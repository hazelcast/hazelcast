/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.nio.serialization.Data;

import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;

/**
 * Handles near-cache invalidations
 */
public abstract class InvalidationHandler {

    public abstract void handle(Data key, String sourceUuid, UUID partitionUuid, long sequence);

    public void handle(Collection<Data> keys, Collection<String> sourceUuids,
                       Collection<UUID> partitionUuids, Collection<Long> sequences) {
        Iterator<Data> keyIterator = keys.iterator();
        Iterator<Long> sequenceIterator = sequences.iterator();
        Iterator<UUID> partitionUuidIterator = partitionUuids.iterator();
        Iterator<String> sourceUuidsIterator = sourceUuids.iterator();

        do {
            if (!(keyIterator.hasNext() && sequenceIterator.hasNext()
                    && partitionUuidIterator.hasNext() && sourceUuidsIterator.hasNext())) {
                break;
            }

            handle(keyIterator.next(), sourceUuidsIterator.next(), partitionUuidIterator.next(), sequenceIterator.next());

        } while (true);
    }
}
