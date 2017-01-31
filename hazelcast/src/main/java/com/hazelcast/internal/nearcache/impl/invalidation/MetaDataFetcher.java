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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.InternalCompletableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * Runs on near-cache side, an instance of this task is responsible for fetching of all near-caches' remote metadata like last
 * sequence numbers and partition-uuids. To see usage of this metadata visit: {@link MetaDataGenerator}.
 * <p>
 * This class is abstract to provide different implementations on client and member sides.
 */
public abstract class MetaDataFetcher {

    protected final ILogger logger;

    public MetaDataFetcher(ILogger logger) {
        this.logger = logger;
    }

    protected abstract List<InternalCompletableFuture> scanMembers(List<String> names);

    protected abstract void process(InternalCompletableFuture future, ConcurrentMap<String, RepairingHandler> handlers);

    /**
     * Gets or assigns partition-uuids before start of {@link RepairingTask} and returns a list of partition-id,
     * partition-uuid pairs.
     * <p>
     * This method is likely to be called only one time during the life-cycle of a client.
     *
     * @return list of partition-id, partition-uuid pairs for initialization
     * @throws Exception possible exceptions raised by remote calls
     */
    protected abstract List<Object> assignAndGetUuids() throws Exception;

    public final void fetchMetadata(ConcurrentMap<String, RepairingHandler> handlers) {
        if (handlers.isEmpty()) {
            return;
        }

        List<String> mapNames = getNames(handlers);
        List<InternalCompletableFuture> futures = scanMembers(mapNames);
        for (InternalCompletableFuture future : futures) {
            process(future, handlers);
        }
    }

    private List<String> getNames(ConcurrentMap<String, RepairingHandler> handlers) {
        List<String> names = new ArrayList<String>(handlers.size());
        for (RepairingHandler handler : handlers.values()) {
            names.add(handler.getName());
        }
        return names;
    }

    protected void repairUuids(List<Object> uuids, ConcurrentMap<String, RepairingHandler> handlers) {
        for (int i = 0; i < uuids.size(); ) {
            int partitionId = (Integer) uuids.get(i++);
            long mostSigBits = (Long) uuids.get(i++);
            long leastSigBits = (Long) uuids.get(i++);

            for (RepairingHandler handler : handlers.values()) {
                handler.checkOrRepairUuid(partitionId, new UUID(mostSigBits, leastSigBits));
            }
        }
    }

    protected void repairSequences(List<Object> namePartitionSequenceList, ConcurrentMap<String, RepairingHandler> handlers) {
        String name = null;

        for (int i = 0; i < namePartitionSequenceList.size(); ) {
            Object item = namePartitionSequenceList.get(i++);

            if (item instanceof String) {
                name = ((String) item);
            } else {
                int partitionId = (Integer) item;
                long nextSequence = (Long) namePartitionSequenceList.get(i++);

                RepairingHandler repairingHandler = handlers.get(name);
                repairingHandler.checkOrRepairSequence(partitionId, nextSequence, true);
            }
        }
    }
}
