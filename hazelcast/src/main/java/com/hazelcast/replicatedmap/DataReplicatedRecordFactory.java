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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.record.ReplicatedRecordFactory;
import com.hazelcast.replicatedmap.record.Vector;
import com.hazelcast.spi.NodeEngine;

class DataReplicatedRecordFactory implements ReplicatedRecordFactory<Data> {

    private final NodeEngine nodeEngine;
    private final InMemoryFormat inMemoryFormat;
    private final int localMemberHashCode;

    DataReplicatedRecordFactory(InMemoryFormat inMemoryFormat, NodeEngine nodeEngine) {
        this.inMemoryFormat = inMemoryFormat;
        this.nodeEngine = nodeEngine;
        this.localMemberHashCode = nodeEngine.getLocalMember().getUuid().hashCode();
    }

    @Override
    public ReplicatedRecord<Data> newRecord(Data key, Object value) {
        Data v = nodeEngine.toData(value);
        return new DataReplicatedRecord(key, v, new Vector(), localMemberHashCode);
    }

    @Override
    public void setValue(ReplicatedRecord<Data> record, Object value) {
        Data v = nodeEngine.toData(value);
        record.setValue(v, localMemberHashCode);
    }

    @Override
    public boolean equals(Object value1, Object value2) {
        return nodeEngine.toData(value1).equals(nodeEngine.toData(value2));
    }

    @Override
    public InMemoryFormat getStorageFormat() {
        return inMemoryFormat;
    }
}
