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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.NodeEngine;

/**
 * This is a {@link com.hazelcast.nio.serialization.Data} based {@link ReplicatedRecordStore}
 * implementation
 */
public class DataReplicatedRecordStore extends AbstractReplicatedRecordStore<Data, Data> {

    private final NodeEngine nodeEngine;

    public DataReplicatedRecordStore(String name, ReplicatedMapService replicatedMapService) {
        super(name, replicatedMapService);
        this.nodeEngine = replicatedMapService.getNodeEngine();
    }

    @Override
    public Object unmarshallKey(Object key) {
        return key == null ? null : nodeEngine.toObject(key);
    }

    @Override
    public Object unmarshallValue(Object value) {
        return value == null ? null : nodeEngine.toObject(value);
    }

    @Override
    public Object marshallKey(Object key) {
        return key == null ? null : nodeEngine.toData(key);
    }

    @Override
    public Object marshallValue(Object value) {
        return value == null ? null : nodeEngine.toData(value);
    }

}
