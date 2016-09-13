/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cardinality.operations;

import com.hazelcast.cardinality.CardinalityEstimatorContainer;
import com.hazelcast.cardinality.CardinalityEstimatorDataSerializerHook;
import com.hazelcast.cardinality.CardinalityEstimatorService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReplicationOperation extends Operation
        implements IdentifiedDataSerializable {

    private Map<String, CardinalityEstimatorContainer> migrationData;

    public ReplicationOperation() {
    }

    public ReplicationOperation(Map<String, CardinalityEstimatorContainer> migrationData) {
        this.migrationData = migrationData;
    }

    @Override
    public void run() throws Exception {
        CardinalityEstimatorService service = getService();
        for (Map.Entry<String, CardinalityEstimatorContainer> entry : migrationData.entrySet()) {
            String name = entry.getKey();
            CardinalityEstimatorContainer newContainer = service.getCardinalityEstimatorContainer(name);
            newContainer.setStore(entry.getValue().getStore());
        }
    }

    @Override
    public String getServiceName() {
        return CardinalityEstimatorService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return CardinalityEstimatorDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CardinalityEstimatorDataSerializerHook.REPLICATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Map.Entry<String, CardinalityEstimatorContainer> entry : migrationData.entrySet()) {
            out.writeUTF(entry.getKey());
            entry.getValue().writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int mapSize = in.readInt();
        migrationData = new HashMap<String, CardinalityEstimatorContainer>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            String name = in.readUTF();
            CardinalityEstimatorContainer newCont = new CardinalityEstimatorContainer();
            newCont.readData(in);
            migrationData.put(name, newCont);
        }
    }
}
