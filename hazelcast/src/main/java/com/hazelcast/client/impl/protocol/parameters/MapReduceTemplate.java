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

package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.annotation.EncodeMethod;
import com.hazelcast.annotation.GenerateParameters;
import com.hazelcast.nio.serialization.Data;

import java.util.List;

@GenerateParameters(id = 14, name = "MapReduce", ns = "Hazelcast.Client.Protocol.MapReduce")
public interface MapReduceTemplate {

    @EncodeMethod(id = 1)
    void cancel(String name, String jobId);

    @EncodeMethod(id = 2)
    void jobProcessInformation(String name, String jobId);

    @EncodeMethod(id = 3)
    void forMap(String name, String jobId, Data predicate, Data mapper, Data combinerFactory,
                Data reducerFactory, String mapName, int chunkSize, List<Data> keys,
                String topologyChangedStrategy);

    @EncodeMethod(id = 4)
    void forList(String name, String jobId, Data predicate, Data mapper, Data combinerFactory,
                 Data reducerFactory, String listName, int chunkSize, List<Data> keys,
                 String topologyChangedStrategy);


    @EncodeMethod(id = 5)
    void forSet(String name, String jobId, Data predicate, Data mapper, Data combinerFactory,
                Data reducerFactory, String setName, int chunkSize, List<Data> keys,
                String topologyChangedStrategy);

    @EncodeMethod(id = 6)
    void forMultiMap(String name, String jobId, Data predicate, Data mapper, Data combinerFactory,
                     Data reducerFactory, String multiMapName, int chunkSize, List<Data> keys,
                     String topologyChangedStrategy);

    @EncodeMethod(id = 7)
    void forCustom(String name, String jobId, Data predicate, Data mapper, Data combinerFactory,
                   Data reducerFactory, Data keyValueSource, int chunkSize, List<Data> keys,
                   String topologyChangedStrategy);

}

