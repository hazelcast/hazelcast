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
import com.hazelcast.annotation.Nullable;
import com.hazelcast.annotation.Request;
import com.hazelcast.client.impl.protocol.ResponseMessageConst;
import com.hazelcast.nio.serialization.Data;

import java.util.List;

@GenerateCodec(id = TemplateConstants.MAP_REDUCE_TEMPLATE_ID, name = "MapReduce", ns = "Hazelcast.Client.Protocol.Codec")
public interface MapReduceCodecTemplate {

    /**
     *
     * @param name Name of the distributed object
     * @param jobId Id of the job to cancel
     * @return Returns true if successful, false otherwise
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object cancel(String name, String jobId);

    /**
     *
     * @param name Name of the distributed object
     * @param jobId Id of the job
     * @return The information about the job if exists
     */
    @Request(id = 2, retryable = true, response = ResponseMessageConst.JOB_PROCESS_INFO)
    Object jobProcessInformation(String name, String jobId);

    /**
     *
     * @param name Name of the distributed object
     * @param jobId Id of the job
     * @param predicate The filter to use during operation
     * @param mapper The mapper for the operation
     * @param combinerFactory The combiner factory to use
     * @param reducerFactory The reducer factory to be used
     * @param mapName Name of the Map object to work on.
     * @param chunkSize The number of items for which the reduce shall be performed
     * @param keys The keys for the objects to be processed
     * @param topologyChangedStrategy The strategy to use if a topology change is detected.
     * @return The resulting key-value pairs.
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.LIST_ENTRY)
    Object forMap(String name, String jobId, @Nullable Data predicate, Data mapper, @Nullable Data combinerFactory,
                @Nullable Data reducerFactory, String mapName, int chunkSize, @Nullable List<Data> keys,
                @Nullable String topologyChangedStrategy);

    /**
     *
     * @param name Name of the distributed object
     * @param jobId Id of the job
     * @param predicate The filter to use during operation
     * @param mapper The mapper for the operation
     * @param combinerFactory The combiner factory to use
     * @param reducerFactory The reducer factory to be used
     * @param listName Name of the List object to work on.
     * @param chunkSize The number of items for which the reduce shall be performed
     * @param keys The keys for the objects to be processed
     * @param topologyChangedStrategy The strategy to use if a topology change is detected.
     * @return The resulting key-value pairs.
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.LIST_ENTRY)
    Object forList(String name, String jobId, @Nullable Data predicate, Data mapper, @Nullable Data combinerFactory,
                 @Nullable Data reducerFactory, String listName, int chunkSize, @Nullable List<Data> keys,
                 @Nullable String topologyChangedStrategy);

    /**
     *
     * @param name Name of the distributed object
     * @param jobId Id of the job
     * @param predicate The filter to use during operation
     * @param mapper The mapper for the operation
     * @param combinerFactory The combiner factory to use
     * @param reducerFactory The reducer factory to be used
     * @param setName Name of the Set object to work on.
     * @param chunkSize The number of items for which the reduce shall be performed
     * @param keys The keys for the objects to be processed
     * @param topologyChangedStrategy The strategy to use if a topology change is detected.
     * @return The resulting key-value pairs.
     */
    @Request(id = 5, retryable = false, response = ResponseMessageConst.LIST_ENTRY)
    Object forSet(String name, String jobId, @Nullable Data predicate, Data mapper, @Nullable Data combinerFactory,
                @Nullable Data reducerFactory, String setName, int chunkSize, @Nullable List<Data> keys,
                @Nullable String topologyChangedStrategy);

    /**
     *
     * @param name Name of the distributed object
     * @param jobId Id of the job
     * @param predicate The filter to use during operation
     * @param mapper The mapper for the operation
     * @param combinerFactory The combiner factory to use
     * @param reducerFactory The reducer factory to be used
     * @param multiMapName Name of the MultiMap object to work on.
     * @param chunkSize The number of items for which the reduce shall be performed
     * @param keys The keys for the objects to be processed
     * @param topologyChangedStrategy The strategy to use if a topology change is detected.
     * @return The resulting key-value pairs.
     */
    @Request(id = 6, retryable = false, response = ResponseMessageConst.LIST_ENTRY)
    Object forMultiMap(String name, String jobId, @Nullable Data predicate, Data mapper, @Nullable Data combinerFactory,
                     @Nullable Data reducerFactory, String multiMapName, int chunkSize, @Nullable List<Data> keys,
                     @Nullable String topologyChangedStrategy);

    /**
     *
     * @param name Name of the distributed object
     * @param jobId Id of the job
     * @param predicate The filter to use during operation
     * @param mapper The mapper for the operation
     * @param combinerFactory The combiner factory to use
     * @param reducerFactory The reducer factory to be used
     * @param keyValueSource custom data sources for mapreduce algorithm. The object implements the
     *                       com.hazelcast.mapreduce.KeyValueSource interface
     * @param chunkSize The number of items for which the reduce shall be performed
     * @param keys The keys for the objects to be processed
     * @param topologyChangedStrategy The strategy to use if a topology change is detected.
     * @return The resulting key-value pairs.
     */
    @Request(id = 7, retryable = false, response = ResponseMessageConst.LIST_ENTRY)
    Object forCustom(String name, String jobId, @Nullable Data predicate, Data mapper, @Nullable Data combinerFactory,
                   @Nullable Data reducerFactory, Data keyValueSource, int chunkSize, @Nullable List<Data> keys,
                   @Nullable String topologyChangedStrategy);
}
