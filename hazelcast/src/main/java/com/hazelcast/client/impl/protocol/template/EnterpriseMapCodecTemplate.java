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

    /**
     *
     * @param mapName Name of the map.
     * @param cacheName Name of the cache for query cache.
     * @param predicate The predicate to filter events which will be applied to the QueryCache.
     * @param batchSize The size of batch. After reaching this minimum size, node immediately sends buffered events to QueryCache.
     * @param bufferSize Maximum number of events which can be stored in a buffer of partition.
     * @param delaySeconds The minimum number of delay seconds which an event waits in the buffer of node.
     * @param populate Flag to enable/disable initial population of the QueryCache.
     * @param coalesce Flag to enable/disable coalescing. If true, then only the last updated value for a key is placed in the
     *                 batch, otherwise all changed values are included in the update.
     * @return Array of key-value pairs.
     */
    @Request(id = 1, retryable = true, response = ResponseMessageConst.SET_ENTRY)
    Object publisherCreateWithValue(String mapName, String cacheName, Data predicate, int batchSize, int bufferSize,
                                  long delaySeconds, boolean populate, boolean coalesce);

    /**
     *
     * @param mapName Name of the map.
     * @param cacheName Name of query cache.
     * @param predicate The predicate to filter events which will be applied to the QueryCache.
     * @param batchSize The size of batch. After reaching this minimum size, node immediately sends buffered events to QueryCache.
     * @param bufferSize Maximum number of events which can be stored in a buffer of partition.
     * @param delaySeconds The minimum number of delay seconds which an event waits in the buffer of node.
     * @param populate Flag to enable/disable initial population of the QueryCache.
     * @param coalesce Flag to enable/disable coalescing. If true, then only the last updated value for a key is placed in the
     *                 batch, otherwise all changed values are included in the update.
     * @return Array of keys.
     */
    @Request(id = 2, retryable = true, response = ResponseMessageConst.SET_DATA)
    Object publisherCreate(String mapName, String cacheName, Data predicate, int batchSize, int bufferSize, long delaySeconds,
                         boolean populate, boolean coalesce);

    /**
     *
     * @param mapName Name of the map.
     * @param cacheName Name of query cache.
     * @return True if successfully set as publishable, false otherwise.
     */
    @Request(id = 3, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object madePublishable(String mapName, String cacheName);

    /**
     *
     * @param listenerName Name of the MapListener which will be used to listen this QueryCache
     * @return Registration id for the listener.
     */
    @Request(id = 4, retryable = true, response = ResponseMessageConst.STRING,
             event = {EventMessageConst.EVENT_QUERYCACHESINGLE, EventMessageConst.EVENT_QUERYCACHEBATCH})
    Object addListener(String listenerName);

    /**
     *
     * @param mapName Name of the map.
     * @param cacheName Name of query cache.
     * @param sequence The cursor position of the accumulator to be set.
     * @return True if the cursor position could be set, false otherwise.
     */
    @Request(id = 5, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object setReadCursor(String mapName, String cacheName, long sequence);

    /**
     *
     * @param mapName Name of the map.
     * @param cacheName Name of query cache.
     * @return True if all cache is destroyed, false otherwise.
     */
    @Request(id = 6, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object destroyCache(String mapName, String cacheName);

}
