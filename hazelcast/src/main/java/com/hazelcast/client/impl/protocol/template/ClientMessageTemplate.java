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
import com.hazelcast.client.impl.protocol.EventMessageConst;
import com.hazelcast.client.impl.protocol.ResponseMessageConst;
import com.hazelcast.client.impl.protocol.parameters.TemplateConstants;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

@GenerateCodec(id = TemplateConstants.CLIENT_TEMPLATE_ID, name = "Client", ns = "Hazelcast.Client.Protocol.Internal")
public interface ClientMessageTemplate {

    @Request(id = 1, retryable = true, response = ResponseMessageConst.AUTHENTICATION)
    void authentication(String username, String password, String uuid, String ownerUuid, boolean isOwnerConnection);

    @Request(id = 2, retryable = true, response = ResponseMessageConst.AUTHENTICATION)
    void authenticationCustom(Data credentials, String uuid, String ownerUuid, boolean isOwnerConnection);

    @Request(id = 8, retryable = false, response = ResponseMessageConst.STRING,
            event = {EventMessageConst.EVENT_MEMBER, EventMessageConst.EVENT_MEMBERLIST, EventMessageConst.EVENT_MEMBERATTRIBUTECHANGE})
    void membershipListener();

    @Request(id = 11, retryable = false, response = ResponseMessageConst.VOID)
    void createProxy(String name, String serviceName);

    @Request(id = 12, retryable = false, response = ResponseMessageConst.VOID)
    void destroyProxy(String name, String serviceName);

    @Request(id = 17, retryable = false, response = ResponseMessageConst.STRING, event = EventMessageConst.EVENT_ITEM)
    void itemListener(String name, boolean includeValue);

    @Request(id = 18, retryable = false, response = ResponseMessageConst.PARTITIONS)
    void getPartitions();

    @Request(id = 19, retryable = false, response = ResponseMessageConst.VOID)
    void removeAllListeners();

    @Request(id = 20, retryable = false, response = ResponseMessageConst.STRING, event = {EventMessageConst.EVENT_PARTITIONLOSTEVENT})
    void addPartitionLostListener(int partitionId, int lostBackupCount, Address source);

    @Request(id = 21, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void removePartitionLostListener(String registrationId);

    @Request(id = 22, retryable = false, response = ResponseMessageConst.DISTRIBUTED_OBJECT)
    void getDistributedObject();

    @Request(id = 23, retryable = false, response = ResponseMessageConst.STRING, event = {EventMessageConst.EVENT_DISTRIBUTEDOBJECT})
    void addDistributedObjectListener();

    @Request(id = 24, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void removeDistributedObjectListener(String registrationId);

    @Request(id = 25, retryable = true, response = ResponseMessageConst.VOID)
    void ping();

}
