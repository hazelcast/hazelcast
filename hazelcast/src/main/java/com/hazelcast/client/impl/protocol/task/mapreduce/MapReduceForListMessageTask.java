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

package com.hazelcast.client.impl.protocol.task.mapreduce;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.MapReduceForListParameters;
import com.hazelcast.instance.Node;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.impl.ListKeyValueSource;
import com.hazelcast.nio.Connection;

import java.util.Collection;

public class MapReduceForListMessageTask extends AbstractMapReduceTask<MapReduceForListParameters> {

    public MapReduceForListMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected String getJobId() {
        return parameters.jobId;
    }

    @Override
    protected int getChunkSize() {
        return parameters.chunkSize;
    }

    @Override
    protected String getTopologyChangedStrategy() {
        return parameters.topologyChangedStrategy;
    }

    @Override
    protected KeyValueSource getKeyValueSource() {
        return new ListKeyValueSource(parameters.listName);
    }

    @Override
    protected Mapper getMapper() {
        return serializationService.toObject(parameters.mapper);
    }

    @Override
    protected CombinerFactory getCombinerFactory() {
        return serializationService.toObject(parameters.combinerFactory);
    }

    @Override
    protected ReducerFactory getReducerFactory() {
        return serializationService.toObject(parameters.reducerFactory);
    }

    @Override
    protected Collection getKeys() {
        return parameters.keys;
    }

    @Override
    protected KeyPredicate getPredicate() {
        return serializationService.toObject(parameters.predicate);
    }

    @Override
    protected MapReduceForListParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapReduceForListParameters.decode(clientMessage);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }
}
