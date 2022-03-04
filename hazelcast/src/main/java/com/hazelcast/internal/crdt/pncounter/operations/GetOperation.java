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

package com.hazelcast.internal.crdt.pncounter.operations;

import com.hazelcast.cluster.impl.VectorClock;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.internal.crdt.CRDTDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

import java.io.IOException;

/**
 * Query operation to retrieve the current value of the
 * {@link PNCounter}.
 * The session consistency guarantees are provided by sending a last-observed
 * vector clock with each operation. The operation response will contain the
 * current replica vector clock.
 */
public class GetOperation extends AbstractPNCounterOperation implements ReadonlyOperation {
    private VectorClock observedTimestamps;
    private CRDTTimestampedLong response;

    /**
     * Constructs the operation.
     *
     * @param name name of the {@link PNCounter}
     */
    public GetOperation(String name, VectorClock observedClock) {
        super(name);
        this.observedTimestamps = observedClock;
    }

    public GetOperation() {
    }

    @Override
    public void run() throws Exception {
        response = getPNCounter(observedTimestamps).get(observedTimestamps);
    }

    @Override
    public CRDTTimestampedLong getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(observedTimestamps);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        observedTimestamps = in.readObject();
    }

    @Override
    public int getClassId() {
        return CRDTDataSerializerHook.PN_COUNTER_GET_OPERATION;
    }
}
