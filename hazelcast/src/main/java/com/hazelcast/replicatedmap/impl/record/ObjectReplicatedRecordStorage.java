/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.replicatedmap.impl.ReplicatedMapService;

/**
 * This is a {@link com.hazelcast.config.InMemoryFormat#OBJECT} based
 * {@link ReplicatedRecordStore} implementation
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ObjectReplicatedRecordStorage<K, V> extends AbstractReplicatedRecordStore<K, V> {

    public ObjectReplicatedRecordStorage(String name, ReplicatedMapService replicatedMapService, int partitionId) {
        super(name, replicatedMapService, partitionId);
    }

    @Override
    public Object unmarshall(Object key) {
        return nodeEngine.toObject(key);
    }

    @Override
    public Object marshall(Object key) {
        return nodeEngine.toObject(key);
    }

}
