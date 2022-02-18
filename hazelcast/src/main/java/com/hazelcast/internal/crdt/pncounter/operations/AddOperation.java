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
import com.hazelcast.internal.crdt.pncounter.PNCounterImpl;
import com.hazelcast.internal.crdt.pncounter.PNCounterService;
import com.hazelcast.internal.monitor.impl.LocalPNCounterStatsImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;

/**
 * Addition/subtraction operation for a
 * {@link PNCounter}.
 * The session consistency guarantees are provided by sending a last-observed
 * vector clock with each operation. The operation response will contain the
 * current replica vector clock.
 */
public class AddOperation extends AbstractPNCounterOperation implements MutatingOperation {
    private VectorClock observedTimestamps;
    private boolean getBeforeUpdate;
    private long delta;
    private CRDTTimestampedLong response;

    /**
     * Creates the addition operation.
     *
     * @param name            the name of the PNCounter
     * @param delta           the delta to add to the counter value, can be negative
     * @param getBeforeUpdate {@code true} if the operation should return the
     *                        counter value before the addition, {@code false}
     *                        if it should return the value after the addition
     * @param observedClock   previously observed vector clock, may be {@code null}
     */
    public AddOperation(String name, long delta, boolean getBeforeUpdate, VectorClock observedClock) {
        super(name);
        this.delta = delta;
        this.getBeforeUpdate = getBeforeUpdate;
        this.observedTimestamps = observedClock;
    }

    public AddOperation() {
    }

    @Override
    public void run() throws Exception {
        final PNCounterImpl counter = getPNCounter(observedTimestamps);
        response = getBeforeUpdate
                ? counter.getAndAdd(delta, observedTimestamps)
                : counter.addAndGet(delta, observedTimestamps);
        updateStatistics();
    }

    /** Updates the local PN counter statistics */
    private void updateStatistics() {
        final PNCounterService service = getService();
        final LocalPNCounterStatsImpl stats = service.getLocalPNCounterStats(name);
        if (stats != null) {
            if (delta > 0) {
                stats.incrementIncrementOperationCount();
            } else if (delta < 0) {
                stats.incrementDecrementOperationCount();
            }
            stats.setValue(getBeforeUpdate ? (response.getValue() + delta) : response.getValue());
        }
    }

    @Override
    public CRDTTimestampedLong getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(getBeforeUpdate);
        out.writeLong(delta);
        out.writeObject(observedTimestamps);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        getBeforeUpdate = in.readBoolean();
        delta = in.readLong();
        observedTimestamps = in.readObject();
    }

    @Override
    public int getClassId() {
        return CRDTDataSerializerHook.PN_COUNTER_ADD_OPERATION;
    }
}
