/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.record;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.CleanerRegistrator;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import com.hazelcast.spi.NodeEngine;

import java.util.*;

public class DataReplicatedRecordStore extends AbstractReplicatedRecordStorage<Data, Data> {

    private final NodeEngine nodeEngine;

    public DataReplicatedRecordStore(String name, NodeEngine nodeEngine,
                                     CleanerRegistrator cleanerRegistrator,
                                     ReplicatedMapService replicatedMapService) {
        super(name, nodeEngine, cleanerRegistrator, replicatedMapService);
        this.nodeEngine = nodeEngine;
    }

    @Override
    protected Object unmarshallKey(Object key) {
        return nodeEngine.toObject(key);
    }

    @Override
    protected Object unmarshallValue(Object value) {
        return nodeEngine.toObject(value);
    }

    @Override
    protected Object marshallKey(Object key) {
        return nodeEngine.toData(key);
    }

    @Override
    protected Object marshallValue(Object value) {
        return nodeEngine.toData(value);
    }

}
