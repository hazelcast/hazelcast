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

import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.annotation.Request;
import com.hazelcast.client.impl.protocol.EventResponseMessageType;
import com.hazelcast.client.impl.protocol.ResponseMessageConst;
import com.hazelcast.client.impl.protocol.parameters.TemplateConstants;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

@GenerateCodec(id = TemplateConstants.CLIENT_TEMPLATE_ID, name = "Client", ns = "Hazelcast.Client.Protocol.Internal")
public interface ClientMessageTemplate {

    //    ADD_ENTRY_LISTENER_EVENT(6),
    //    REGISTER_MEMBERSHIP_LISTENER_EVENT(9),

    //    ENTRY_VIEW(23),
    //
    //    ITEM_EVENT(25),
    //    TOPIC_EVENT(26),
    //    PARTITION_LOST_EVENT(28),

    @Request(id = 1, retryable = true, response = ResponseMessageConst.AUTHENTICATION)
    void Authentication(String username, String password, String uuid, String ownerUuid, boolean isOwnerConnection);

    @Request(id = 2, retryable = true, response = ResponseMessageConst.AUTHENTICATION)
    void AuthenticationCustom(Data credentials, String uuid, String ownerUuid, boolean isOwnerConnection);

    @Request(id = 8, retryable = false, response = ResponseMessageConst.STRING,
             event = {EventResponseMessageType.MEMBER_EVENT, EventResponseMessageType.MEMBER_LIST_EVENT, EventResponseMessageType.MEMBER_ATTRIBUTE_EVENT})
    void MembershipListener();

    @Request(id = 11, retryable = false, response = ResponseMessageConst.VOID)
    void CreateProxy(String name, String serviceName);

    @Request(id = 12, retryable = false, response = ResponseMessageConst.VOID)
    void DestroyProxy(String name, String serviceName);

    @Request(id = 17, retryable = false, response = ResponseMessageConst.STRING, event = EventResponseMessageType.ITEM_EVENT)
    void ItemListener(String name, boolean includeValue);

    @Request(id = 18, retryable = false, response = ResponseMessageConst.PARTITIONS)
    void GetPartition();

    @Request(id = 19, retryable = false, response = ResponseMessageConst.VOID)
    void RemoveAllListener();

    @Request(id = 20, retryable = false, response = ResponseMessageConst.VOID)
    void AddPartitionLostListener(int partitionId, int lostBackupCount, Address source);

    @Request(id = 20, retryable = false, response = ResponseMessageConst.VOID)
    void RemovePartitionLostListener(String registrationId);

    @Request(id = 21, retryable = false, response = ResponseMessageConst.DISTRIBUTED_OBJECT)
    void GetDistributedObject();

    @Request(id = 21, retryable = false, response = ResponseMessageConst.STRING)
    void AddDistributedObjectListener();

    @Request(id = 22, retryable = false, response = ResponseMessageConst.VOID)
    void RemoveDistributedObjectListener(String registrationId);

    @Request(id = 23, retryable = true, response = ResponseMessageConst.VOID)
    void Ping();

}
