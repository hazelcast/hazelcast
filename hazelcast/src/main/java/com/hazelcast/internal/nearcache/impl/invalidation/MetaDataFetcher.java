/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * Runs on Near Cache side, an instance of this task is responsible for fetching of all Near Caches' remote metadata like last
 * sequence numbers and partition UUIDs. To see usage of this metadata visit: {@link MetaDataGenerator}.
 *
 * This class is abstract to provide different implementations on client and member sides.
 */
public abstract class MetaDataFetcher {

    protected static final int ASYNC_RESULT_WAIT_TIMEOUT_MINUTES = 1;

    protected final ILogger logger;

    public MetaDataFetcher(ILogger logger) {
        this.logger = logger;
    }

    protected abstract void extractAndPopulateResult(InternalCompletableFuture future, ResultHolder resultHolder)
            throws Exception;

    protected abstract List<InternalCompletableFuture> scanMembers(List<String> names);

    public final void init(RepairingHandler handler) throws Exception {
        ResultHolder resultHolder = new ResultHolder();
        List<InternalCompletableFuture> futures = scanMembers(Collections.singletonList(handler.getName()));
        for (InternalCompletableFuture future : futures) {
            extractAndPopulateResult(future, resultHolder);

            initUuid(resultHolder.partitionUuidList, handler);
            initSequence(resultHolder.namePartitionSequenceList, handler);
        }
    }

    public final void process(InternalCompletableFuture future, ConcurrentMap<String, RepairingHandler> handlers) {
        try {
            ResultHolder resultHolder = new ResultHolder();
            extractAndPopulateResult(future, resultHolder);
            repairUuids(resultHolder.partitionUuidList, handlers);
            repairSequences(resultHolder.namePartitionSequenceList, handlers);
        } catch (Exception e) {
            if (logger.isWarningEnabled()) {
                logger.warning("Can't fetch invalidation meta-data [" + e.getClass().getSimpleName() + "] " + e.getMessage());
            }
        }
    }

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

    protected void repairUuids(Collection<Map.Entry<Integer, UUID>> uuids, ConcurrentMap<String, RepairingHandler> handlers) {
        for (Map.Entry<Integer, UUID> entry : uuids) {
            for (RepairingHandler handler : handlers.values()) {
                handler.checkOrRepairUuid(entry.getKey(), entry.getValue());
            }
        }
    }

    protected void initUuid(Collection<Map.Entry<Integer, UUID>> uuids, RepairingHandler handler) {
        for (Map.Entry<Integer, UUID> entry : uuids) {

            int partitionID = entry.getKey();
            UUID partitionUuid = entry.getValue();

            handler.initUuid(partitionID, partitionUuid);
        }
    }

    protected void repairSequences(Collection<Map.Entry<String, List<Map.Entry<Integer, Long>>>> namePartitionSequenceList,
                                   ConcurrentMap<String, RepairingHandler> handlers) {
        for (Map.Entry<String, List<Map.Entry<Integer, Long>>> entry : namePartitionSequenceList) {
            for (Map.Entry<Integer, Long> subEntry : entry.getValue()) {
                RepairingHandler repairingHandler = handlers.get(entry.getKey());
                repairingHandler.checkOrRepairSequence(subEntry.getKey(), subEntry.getValue(), true);
            }
        }
    }

    protected void initSequence(Collection<Map.Entry<String, List<Map.Entry<Integer, Long>>>> namePartitionSequenceList,
                                RepairingHandler handler) {
        for (Map.Entry<String, List<Map.Entry<Integer, Long>>> entry : namePartitionSequenceList) {
            for (Map.Entry<Integer, Long> subEntry : entry.getValue()) {
                int partitionID = subEntry.getKey();
                long partitionSequence = subEntry.getValue();
                handler.initSequence(partitionID, partitionSequence);
            }
        }
    }

    protected static class ResultHolder {
        private Collection<Map.Entry<String, List<Map.Entry<Integer, Long>>>> namePartitionSequenceList;
        private Collection<Map.Entry<Integer, UUID>> partitionUuidList;

        public void populate(Collection<Map.Entry<Integer, UUID>> partitionUuidList,
                             Collection<Map.Entry<String, List<Map.Entry<Integer, Long>>>> namePartitionSequenceList) {
            this.namePartitionSequenceList = namePartitionSequenceList;
            this.partitionUuidList = partitionUuidList;
        }

        public Collection<Map.Entry<String, List<Map.Entry<Integer, Long>>>> getNamePartitionSequenceList() {
            return namePartitionSequenceList;
        }

        public Collection<Map.Entry<Integer, UUID>> getPartitionUuidList() {
            return partitionUuidList;
        }
    }
}
