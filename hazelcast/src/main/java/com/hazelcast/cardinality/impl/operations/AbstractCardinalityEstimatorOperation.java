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

package com.hazelcast.cardinality.impl.operations;

import com.hazelcast.cardinality.impl.CardinalityEstimatorContainer;
import com.hazelcast.cardinality.impl.CardinalityEstimatorDataSerializerHook;
import com.hazelcast.cardinality.impl.CardinalityEstimatorService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

public abstract class AbstractCardinalityEstimatorOperation
        extends Operation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    protected String name;

    AbstractCardinalityEstimatorOperation() { }

    AbstractCardinalityEstimatorOperation(String name) {
        this.name = name;
    }

    @Override
    public String getServiceName() {
        return CardinalityEstimatorService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return CardinalityEstimatorDataSerializerHook.F_ID;
    }

    public CardinalityEstimatorContainer getCardinalityEstimatorContainer() {
        CardinalityEstimatorService service = getService();
        return service.getCardinalityEstimatorContainer(name);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        this.name = in.readUTF();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", name=").append(name);
    }
}
