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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.HashUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public abstract class AbstractBaseNearCacheInvalidationListener {

    /**
     * This listener is called by member and in the listener we are sending invalidations to client.
     * `batchOrderKey` is used by clients'-striped-executor to find a worker for processing invalidation.
     * By using `batchOrderKey` we are putting all invalidations coming from the same member into the same workers' queue.
     * So if there is more than one member all members will have their own worker to process invalidations. This provides
     * more granular processing.
     */
    private final int batchOrderKey;

    public AbstractBaseNearCacheInvalidationListener(UUID localMemberUuid, long correlationId) {
        this.batchOrderKey = HashUtil.hashCode(localMemberUuid, correlationId);
    }

    protected abstract ClientMessage encodeBatchInvalidation(String name, List<Data> keys, List<UUID> sourceUuids,
                                                             List<UUID> partitionUuids, List<Long> sequences);

    protected abstract ClientMessage encodeSingleInvalidation(String name, Data key, UUID sourceUuid,
                                                              UUID partitionUuid, long sequence);

    protected abstract void sendMessageWithOrderKey(ClientMessage clientMessage, Object orderKey);

    protected abstract boolean canSendInvalidation(Invalidation invalidation);

    protected final void sendInvalidation(Invalidation invalidation) {
        if (invalidation instanceof BatchNearCacheInvalidation) {
            ExtractedParams params = extractParams(((BatchNearCacheInvalidation) invalidation));
            ClientMessage message = encodeBatchInvalidation(invalidation.getName(), params.keys,
                    params.sourceUuids, params.partitionUuids, params.sequences);

            sendMessageWithOrderKey(message, batchOrderKey);
            return;
        }

        if (invalidation instanceof SingleNearCacheInvalidation) {
            if (canSendInvalidation(invalidation)) {
                ClientMessage message = encodeSingleInvalidation(invalidation.getName(), invalidation.getKey(),
                        invalidation.getSourceUuid(), invalidation.getPartitionUuid(), invalidation.getSequence());

                sendMessageWithOrderKey(message, invalidation.getKey());
            }
            return;
        }

        throw new IllegalArgumentException("Unknown invalidation message type " + invalidation);
    }

    private ExtractedParams extractParams(BatchNearCacheInvalidation batch) {
        List<Invalidation> invalidations = batch.getInvalidations();

        int size = invalidations.size();
        List<Data> keys = new ArrayList<>(size);
        List<UUID> sourceUuids = new ArrayList<>(size);
        List<UUID> partitionUuids = new ArrayList<>(size);
        List<Long> sequences = new ArrayList<>(size);

        for (Invalidation invalidation : invalidations) {
            if (canSendInvalidation(invalidation)) {
                keys.add(invalidation.getKey());
                sourceUuids.add(invalidation.getSourceUuid());
                partitionUuids.add(invalidation.getPartitionUuid());
                sequences.add(invalidation.getSequence());
            }
        }

        return new ExtractedParams(keys, sourceUuids, partitionUuids, sequences);
    }

    private static final class ExtractedParams {

        private final List<Data> keys;
        private final List<UUID> sourceUuids;
        private final List<UUID> partitionUuids;
        private final List<Long> sequences;

        ExtractedParams(List<Data> keys, List<UUID> sourceUuids, List<UUID> partitionUuids, List<Long> sequences) {
            this.keys = keys;
            this.sourceUuids = sourceUuids;
            this.partitionUuids = partitionUuids;
            this.sequences = sequences;
        }
    }
}
