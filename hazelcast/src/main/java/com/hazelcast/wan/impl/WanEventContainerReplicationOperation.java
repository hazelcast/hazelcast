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

package com.hazelcast.wan.impl;

import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.internal.cluster.impl.operations.WanOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.wan.WanMigrationAwarePublisher;
import com.hazelcast.wan.WanPublisher;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Replication and migration operation for WAN event containers. This
 * operation is meant to replicate and migrate WAN events between members
 * in a single cluster and not over different clusters.
 * Silently skips publishers not supporting replication.
 *
 * @see WanOperation
 */
public class WanEventContainerReplicationOperation extends Operation implements IdentifiedDataSerializable {
    private Collection<WanReplicationConfig> wanReplicationConfigs;
    private Map<String, Map<String, Object>> eventContainers;

    public WanEventContainerReplicationOperation() {
    }

    public WanEventContainerReplicationOperation(@Nonnull Collection<WanReplicationConfig> wanReplicationConfigs,
                                                 @Nonnull Map<String, Map<String, Object>> eventContainers,
                                                 int partitionId,
                                                 int replicaIndex) {
        checkNotNull(wanReplicationConfigs);
        checkNotNull(eventContainers);
        this.wanReplicationConfigs = wanReplicationConfigs;
        this.eventContainers = eventContainers;
        setPartitionId(partitionId).setReplicaIndex(replicaIndex);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() throws Exception {
        WanReplicationService service = getWanReplicationService();
        int partitionId = getPartitionId();

        for (WanReplicationConfig wanReplicationConfig : wanReplicationConfigs) {
            service.addWanReplicationConfigLocally(wanReplicationConfig);
        }

        // first ensure all publishers have configuration
        forAllReplicationContainers((publisher, o) -> {
        });

        // then ingest replication data
        forAllReplicationContainers((publisher, eventContainer) -> {
            if (publisher instanceof WanMigrationAwarePublisher) {
                ((WanMigrationAwarePublisher) publisher)
                        .processEventContainerReplicationData(partitionId, eventContainer);
            }
        });
    }

    private void forAllReplicationContainers(BiConsumer<WanPublisher, Object> publisherContainerConsumer) {
        WanReplicationService service = getWanReplicationService();
        for (Entry<String, Map<String, Object>> wanReplicationSchemeEntry : eventContainers.entrySet()) {
            String wanReplicationScheme = wanReplicationSchemeEntry.getKey();
            Map<String, Object> eventContainersByPublisherId = wanReplicationSchemeEntry.getValue();
            for (Entry<String, Object> publisherEventContainer : eventContainersByPublisherId.entrySet()) {
                String publisherId = publisherEventContainer.getKey();
                Object eventContainer = publisherEventContainer.getValue();
                WanPublisher publisher = service.getPublisherOrFail(wanReplicationScheme, publisherId);
                publisherContainerConsumer.accept(publisher, eventContainer);
            }
        }
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.WAN_EVENT_CONTAINER_REPLICATION_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(eventContainers.size());
        for (Entry<String, Map<String, Object>> entry : eventContainers.entrySet()) {
            String wanReplicationScheme = entry.getKey();
            Map<String, Object> eventContainersByPublisherId = entry.getValue();
            out.writeString(wanReplicationScheme);
            out.writeInt(eventContainersByPublisherId.size());
            for (Entry<String, Object> publisherEventContainer : eventContainersByPublisherId.entrySet()) {
                String publisherId = publisherEventContainer.getKey();
                Object eventContainer = publisherEventContainer.getValue();
                out.writeString(publisherId);
                out.writeObject(eventContainer);
            }
        }

        out.writeInt(wanReplicationConfigs.size());
        for (WanReplicationConfig wanReplicationConfig : wanReplicationConfigs) {
            out.writeObject(wanReplicationConfig);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int wanReplicationSchemeCount = in.readInt();
        eventContainers = createHashMap(wanReplicationSchemeCount);
        for (int i = 0; i < wanReplicationSchemeCount; i++) {
            String wanReplicationScheme = in.readString();
            int publisherCount = in.readInt();
            Map<String, Object> eventContainersByPublisherId = createHashMap(publisherCount);
            for (int j = 0; j < publisherCount; j++) {
                String publisherId = in.readString();
                Object eventContainer = in.readObject();
                eventContainersByPublisherId.put(publisherId, eventContainer);
            }
            eventContainers.put(wanReplicationScheme, eventContainersByPublisherId);
        }

        int wanConfigCount = in.readInt();
        wanReplicationConfigs = new ArrayList<>(wanConfigCount);
        for (int i = 0; i < wanConfigCount; i++) {
            wanReplicationConfigs.add(in.readObject());
        }
    }

    private WanReplicationService getWanReplicationService() {
        return getNodeEngine().getWanReplicationService();
    }
}
