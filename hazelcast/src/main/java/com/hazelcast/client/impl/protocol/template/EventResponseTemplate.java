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
import com.hazelcast.client.impl.protocol.EventMessageConst;
import com.hazelcast.cluster.client.MemberAttributeChange;
import com.hazelcast.nio.serialization.Data;

import java.util.Collection;

/**
 * Client Protocol Event Responses
 */
@GenerateCodec(id = 0, name = "Event", ns = "")
public interface EventResponseTemplate {

    @EventResponse(EventMessageConst.EVENT_MEMBER)
    void Member(com.hazelcast.instance.AbstractMember member, int eventType);

    @EventResponse(EventMessageConst.EVENT_MEMBERATTRIBUTECHANGE)
    void MemberAttributeChange(MemberAttributeChange memberAttributeChange);

    @EventResponse(EventMessageConst.EVENT_MEMBERLIST)
    void MemberList(Collection<com.hazelcast.instance.AbstractMember> members);

    @EventResponse(EventMessageConst.EVENT_ENTRY)
    void Entry(Data key, Data value, Data oldValue, Data mergingValue, int eventType, String uuid, int numberOfAffectedEntries);

    @EventResponse(EventMessageConst.EVENT_ITEM)
    void Item(Data item, String uuid, int eventType);

    @EventResponse(EventMessageConst.EVENT_TOPIC)
    void Topic(Data item, long publishTime, String uuid);

    @EventResponse(EventMessageConst.EVENT_PARTITIONLOSTEVENT)
    void PartitionLostEvent(int partitionId, String uuid);

    @EventResponse(EventMessageConst.EVENT_DISTRIBUTEDOBJECT)
    void DistributedObject(String name, String serviceName, int eventType);

    @EventResponse(EventMessageConst.EVENT_CACHEINVALIDATION)
    void CacheInvalidation(String name, Data key, String sourceUuid);

}
