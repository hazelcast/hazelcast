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

package com.hazelcast.cp.internal.datastructures.countdownlatch.operation;

import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.cp.internal.RaftOp;

import java.io.IOException;

/**
 * Base class for operations of Raft-based count down latch
 */
public abstract class AbstractCountDownLatchOp extends RaftOp implements IdentifiedDataSerializable {

    protected String name;

    public AbstractCountDownLatchOp() {
    }

    AbstractCountDownLatchOp(String name) {
        this.name = name;
    }

    @Override
    public final String getServiceName() {
        return CountDownLatchService.SERVICE_NAME;
    }

    @Override
    public final int getFactoryId() {
        return CountDownLatchDataSerializerHook.F_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", name=").append(name);
    }
}
