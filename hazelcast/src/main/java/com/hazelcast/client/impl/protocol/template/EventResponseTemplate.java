/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.template;

import com.hazelcast.annotation.EventResponse;
import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.annotation.Nullable;
import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.client.impl.protocol.EventMessageConst;
import com.hazelcast.cluster.client.MemberAttributeChange;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

import java.util.List;
import java.util.Set;

/**
 * Client Protocol Event Responses
 */
@GenerateCodec(id = 0, name = "Event", ns = "")
public interface EventResponseTemplate {

    @EventResponse(EventMessageConst.EVENT_MEMBER)
    void Member(Member member, int eventType);

    @EventResponse(EventMessageConst.EVENT_MEMBERATTRIBUTECHANGE)
    void MemberAttributeChange(MemberAttributeChange memberAttributeChange);

    @EventResponse(EventMessageConst.EVENT_MEMBERSET)
    void MemberSet(Set<Member> members);

    @EventResponse(EventMessageConst.EVENT_ENTRY)
    void Entry(@Nullable Data key, @Nullable Data value, @Nullable Data oldValue, @Nullable Data mergingValue, int eventType,
               String uuid, int numberOfAffectedEntries);

    @EventResponse(EventMessageConst.EVENT_ITEM)
    void Item(@Nullable Data item, String uuid, int eventType);

    @EventResponse(EventMessageConst.EVENT_TOPIC)
    void Topic(Data item, long publishTime, String uuid);

    @EventResponse(EventMessageConst.EVENT_PARTITIONLOST)
    void PartitionLost(int partitionId, int lostBackupCount, @Nullable Address source);

    @EventResponse(EventMessageConst.EVENT_DISTRIBUTEDOBJECT)
    void DistributedObject(String name, String serviceName, String eventType);

    @EventResponse(EventMessageConst.EVENT_CACHEINVALIDATION)
    void CacheInvalidation(String name, @Nullable Data key, @Nullable String sourceUuid);

    @EventResponse(EventMessageConst.EVENT_CACHEBATCHINVALIDATION)
    void CacheBatchInvalidation(String name, List<Data> keys, @Nullable List<String> sourceUuids);

    @EventResponse(EventMessageConst.EVENT_MAPPARTITIONLOST)
    void MapPartitionLost(int partitionId, String uuid);

    @EventResponse(EventMessageConst.EVENT_CACHE)
    void Cache(int type, Set<CacheEventData> keys, int completionId);

}
