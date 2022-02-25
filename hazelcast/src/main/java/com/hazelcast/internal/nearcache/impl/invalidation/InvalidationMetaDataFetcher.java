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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static java.lang.String.format;

/**
 * Runs on Near Cache side, an instance of this task is responsible for fetching of all Near Caches' remote metadata like last
 * sequence numbers and partition UUIDs. To see usage of this metadata visit: {@link MetaDataGenerator}.
 *
 * This class is abstract to provide different implementations on client and member sides.
 */
public abstract class InvalidationMetaDataFetcher {

    protected static final int ASYNC_RESULT_WAIT_TIMEOUT_MINUTES = 1;

    protected final ILogger logger;

    public InvalidationMetaDataFetcher(ILogger logger) {
        this.logger = logger;
    }

    // returns true if handler is initialized properly, otherwise false
    public final boolean init(RepairingHandler handler) {
        MetadataHolder resultHolder = new MetadataHolder();
        List<String> dataStructureNames = Collections.singletonList(handler.getName());
        Map<Member, InternalCompletableFuture> futureByMember = fetchMembersMetadataFor(dataStructureNames);
        for (Map.Entry<Member, InternalCompletableFuture> entry : futureByMember.entrySet()) {
            Member member = entry.getKey();
            InternalCompletableFuture future = entry.getValue();

            try {
                extractMemberMetadata(member, future, resultHolder);
            } catch (Exception e) {
                handleExceptionWhileProcessingMetadata(member, e);
                return false;
            }

            initUuid(resultHolder.partitionUuidList, handler);
            initSequence(resultHolder.namePartitionSequenceList, handler);
        }
        return true;
    }

    public final void fetchMetadata(ConcurrentMap<String, RepairingHandler> handlers) {
        if (handlers.isEmpty()) {
            return;
        }

        List<String> dataStructureNames = getDataStructureNames(handlers);
        Map<Member, InternalCompletableFuture> futureByMember = fetchMembersMetadataFor(dataStructureNames);
        for (Map.Entry<Member, InternalCompletableFuture> entry : futureByMember.entrySet()) {
            Member member = entry.getKey();
            InternalCompletableFuture future = entry.getValue();
            processMemberMetadata(member, future, handlers);
        }
    }

    protected abstract Collection<Member> getDataMembers();

    protected abstract void extractMemberMetadata(Member member,
                                                  InternalCompletableFuture future,
                                                  MetadataHolder metadataHolder) throws Exception;

    protected abstract InternalCompletableFuture fetchMetadataOf(Member member, List<String> names);

    private Map<Member, InternalCompletableFuture> fetchMembersMetadataFor(List<String> names) {
        Collection<Member> members = getDataMembers();
        if (members.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<Member, InternalCompletableFuture> futureByMember = createHashMap(members.size());
        for (Member member : members) {
            Address address = member.getAddress();
            try {
                futureByMember.put(member, fetchMetadataOf(member, names));
            } catch (Exception e) {
                handleExceptionWhileProcessingMetadata(member, e);
            }
        }

        return futureByMember;
    }

    private void processMemberMetadata(Member member, InternalCompletableFuture future,
                                       ConcurrentMap<String, RepairingHandler> handlers) {

        MetadataHolder resultHolder = new MetadataHolder();
        try {
            extractMemberMetadata(member, future, resultHolder);
        } catch (Exception e) {
            handleExceptionWhileProcessingMetadata(member, e);
            return;
        }

        repairUuids(resultHolder.getPartitionUuidList(), handlers);
        repairSequences(resultHolder.getNamePartitionSequenceList(), handlers);
    }

    protected void handleExceptionWhileProcessingMetadata(Member member, Exception e) {
        if (e instanceof IllegalStateException
                || (e instanceof ExecutionException && e.getCause() instanceof IllegalStateException)) {
            // log at finest when
            // HazelcastInstanceNotActive, HazelcastClientNotActive or HazelcastClientOffline exception
            logger.finest(e);
        } else if (logger.isWarningEnabled()) {
            logger.warning(format("Can't fetch or extract invalidation meta-data of %s", member) + ", reason: " + e);
        }
    }

    private List<String> getDataStructureNames(ConcurrentMap<String, RepairingHandler> handlers) {
        List<String> names = new ArrayList<String>(handlers.size());
        for (RepairingHandler handler : handlers.values()) {
            names.add(handler.getName());
        }
        return names;
    }

    private void repairUuids(Collection<Map.Entry<Integer, UUID>> uuids, ConcurrentMap<String, RepairingHandler> handlers) {
        for (Map.Entry<Integer, UUID> entry : uuids) {
            for (RepairingHandler handler : handlers.values()) {
                handler.checkOrRepairUuid(entry.getKey(), entry.getValue());
            }
        }
    }

    private void initUuid(Collection<Map.Entry<Integer, UUID>> uuids, RepairingHandler handler) {
        for (Map.Entry<Integer, UUID> entry : uuids) {

            int partitionID = entry.getKey();
            UUID partitionUuid = entry.getValue();

            handler.initUuid(partitionID, partitionUuid);
        }
    }

    private void repairSequences(Collection<Map.Entry<String, List<Map.Entry<Integer, Long>>>> namePartitionSequenceList,
                                 ConcurrentMap<String, RepairingHandler> handlers) {
        for (Map.Entry<String, List<Map.Entry<Integer, Long>>> entry : namePartitionSequenceList) {
            for (Map.Entry<Integer, Long> subEntry : entry.getValue()) {
                RepairingHandler repairingHandler = handlers.get(entry.getKey());
                repairingHandler.checkOrRepairSequence(subEntry.getKey(), subEntry.getValue(), true);
            }
        }
    }

    private void initSequence(Collection<Map.Entry<String, List<Map.Entry<Integer, Long>>>> namePartitionSequenceList,
                              RepairingHandler handler) {
        for (Map.Entry<String, List<Map.Entry<Integer, Long>>> entry : namePartitionSequenceList) {
            for (Map.Entry<Integer, Long> subEntry : entry.getValue()) {
                int partitionID = subEntry.getKey();
                long partitionSequence = subEntry.getValue();
                handler.initSequence(partitionID, partitionSequence);
            }
        }
    }

    protected static class MetadataHolder {
        private Collection<Map.Entry<Integer, UUID>> partitionUuidList;
        private Collection<Map.Entry<String, List<Map.Entry<Integer, Long>>>> namePartitionSequenceList;

        public void setMetadata(Collection<Map.Entry<Integer, UUID>> partitionUuidList,
                                Collection<Map.Entry<String, List<Map.Entry<Integer, Long>>>> namePartitionSequenceList) {
            this.namePartitionSequenceList = namePartitionSequenceList;
            this.partitionUuidList = partitionUuidList;
        }

        public Collection<Map.Entry<Integer, UUID>> getPartitionUuidList() {
            return partitionUuidList;
        }

        public Collection<Map.Entry<String, List<Map.Entry<Integer, Long>>>> getNamePartitionSequenceList() {
            return namePartitionSequenceList;
        }
    }
}
