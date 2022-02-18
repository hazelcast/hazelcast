/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;

public class AggregateBackupOperation
        extends AbstractCardinalityEstimatorOperation
        implements BackupOperation {

    private long hash;

    public AggregateBackupOperation() {
    }

    public AggregateBackupOperation(String name, long hash) {
        super(name);
        this.hash = hash;
    }

    @Override
    public void run() throws Exception {
        CardinalityEstimatorContainer container = getCardinalityEstimatorContainer();
        container.add(hash);
    }

    @Override
    public int getClassId() {
        return CardinalityEstimatorDataSerializerHook.AGGREGATE_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(hash);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        hash = in.readLong();
    }
}
