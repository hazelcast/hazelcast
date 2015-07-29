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
import com.hazelcast.nio.serialization.Data;

@GenerateCodec(id = TemplateConstants.ENTERPRISE_MAP_TEMPLATE_ID, name = "EnterpriseMap", ns = "Hazelcast.Client.Protocol.Map")
public interface EnterpriseMapCodecTemplate {

    @Request(id = 1, retryable = true, response = ResponseMessageConst.SET_ENTRY)
    void publisherCreateWithValue(String mapName, String cacheName, Data predicate, int batchSize, int bufferSize,
                                  long delaySeconds, boolean populate, boolean coalesce);

    @Request(id = 2, retryable = true, response = ResponseMessageConst.LIST_DATA)
    void publisherCreate(String mapName, String cacheName, Data predicate, int batchSize, int bufferSize, long delaySeconds,
                         boolean populate, boolean coalesce);

    @Request(id = 3, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void madePublishable(String mapName, String cacheName);

    @Request(id = 4, retryable = true, response = ResponseMessageConst.STRING,
             event = {EventMessageConst.EVENT_QUERYCACHESINGLE, EventMessageConst.EVENT_QUERYCACHEBATCH})
    void addListener(String listenerName);

    @Request(id = 5, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void setReadCursor(String mapName, String cacheName, long sequence);

    @Request(id = 6, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void destroyCache(String mapName, String cacheName);

}
