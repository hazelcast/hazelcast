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
import com.hazelcast.client.impl.protocol.EventResponseMessageType;
import com.hazelcast.cluster.client.MemberAttributeChange;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.serialization.Data;

import java.util.Collection;

/**
 * Client Protocol Event Responses
 */
@GenerateCodec(id= 0, name= "Event", ns = "")
public interface EventResponseTemplate {

    @EventResponse(EventResponseMessageType.MEMBER_EVENT)
    void Member(com.hazelcast.client.impl.MemberImpl member, int eventType);

    @EventResponse(EventResponseMessageType.MEMBER_ATTRIBUTE_EVENT)
    void MemberAttributeChange(MemberAttributeChange memberAttributeChange);

    @EventResponse(EventResponseMessageType.MEMBER_LIST_EVENT)
    void MemberList(Collection<com.hazelcast.client.impl.MemberImpl> members);

    @EventResponse(EventResponseMessageType.ITEM_EVENT)
    void ItemEvent(Data item, String uuid, ItemEventType eventType);

    @EventResponse(EventResponseMessageType.TOPIC_EVENT)
    void TopicEvent(Data item, String uuid, ItemEventType eventType);

    @EventResponse(EventResponseMessageType.ENTRY_EVENT)
    void EntryEvent(Data key, Data value, Data oldValue, Data mergingValue, int eventType, String uuid, int numberOfAffectedEntries);

}
