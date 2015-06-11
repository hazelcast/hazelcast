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

@GenerateCodec(id = TemplateConstants.MAP_REDUCE_TEMPLATE_ID, name = "MapReduce", ns = "Hazelcast.Client.Protocol.MapReduce")
public interface MapReduceCodecTemplate {

    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void cancel(String name, String jobId);

    @Request(id = 2, retryable = true, response = ResponseMessageConst.JOB_PROCESS_INFO)
    void jobProcessInformation(String name, String jobId);

    @Request(id = 3, retryable = false, response = ResponseMessageConst.MAP_DATA_DATA)
    void forMap(String name, String jobId, @Nullable Data predicate, Data mapper, @Nullable Data combinerFactory,
                @Nullable Data reducerFactory, String mapName, int chunkSize, @Nullable List<Data> keys,
                @Nullable String topologyChangedStrategy);

    @Request(id = 4, retryable = false, response = ResponseMessageConst.MAP_DATA_DATA)
    void forList(String name, String jobId, @Nullable Data predicate, Data mapper, @Nullable Data combinerFactory,
                 @Nullable Data reducerFactory, String listName, int chunkSize, @Nullable List<Data> keys,
                 @Nullable String topologyChangedStrategy);


    @Request(id = 5, retryable = false, response = ResponseMessageConst.MAP_DATA_DATA)
    void forSet(String name, String jobId, @Nullable Data predicate, Data mapper, @Nullable Data combinerFactory,
                @Nullable Data reducerFactory, String setName, int chunkSize, @Nullable List<Data> keys,
                @Nullable String topologyChangedStrategy);

    @Request(id = 6, retryable = false, response = ResponseMessageConst.MAP_DATA_DATA)
    void forMultiMap(String name, String jobId, @Nullable Data predicate, Data mapper, @Nullable Data combinerFactory,
                     @Nullable Data reducerFactory, String multiMapName, int chunkSize, @Nullable List<Data> keys,
                     @Nullable String topologyChangedStrategy);

    @Request(id = 7, retryable = false, response = ResponseMessageConst.MAP_DATA_DATA)
    void forCustom(String name, String jobId, @Nullable Data predicate, Data mapper, @Nullable Data combinerFactory,
                   @Nullable Data reducerFactory, Data keyValueSource, int chunkSize, @Nullable List<Data> keys,
                   @Nullable String topologyChangedStrategy);
}
