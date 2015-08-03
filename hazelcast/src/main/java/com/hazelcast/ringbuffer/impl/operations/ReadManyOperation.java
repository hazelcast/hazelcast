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

package com.hazelcast.ringbuffer.impl.operations;

import com.hazelcast.core.IFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.client.PortableReadResultSet;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.READ_MANY_OPERATION;

public class ReadManyOperation extends AbstractRingBufferOperation implements WaitSupport {

    long startSequence;
    transient ReadResultSetImpl resultSet;
    transient long sequence;

    private boolean returnPortable;
    private int minSize;
    private int maxSize;
    private IFunction<Object, Boolean> filter;

    public ReadManyOperation() {
    }

    public ReadManyOperation(String name, long startSequence, int minSize, int maxSize, IFunction<?, Boolean> filter) {
        this(name, startSequence, minSize, maxSize, filter, false);
    }

    public ReadManyOperation(String name, long startSequence, int minSize, int maxSize, IFunction<?, Boolean> filter,
                             boolean returnPortable) {
        super(name);
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.startSequence = startSequence;
        this.filter = (IFunction<Object, Boolean>) filter;
        this.returnPortable = returnPortable;
    }

    @Override
    public void beforeRun() {
        RingbufferContainer ringbuffer = getRingBufferContainer();
        ringbuffer.checkBlockableReadSequence(startSequence);
    }

    @Override
    public boolean shouldWait() {
        if (resultSet == null) {
            resultSet = new ReadResultSetImpl(minSize, maxSize, getNodeEngine().getHazelcastInstance(), filter);
            sequence = startSequence;
        }

        if (resultSet.isMinSizeReached()) {
            // if the minimum number of items are found, we don't need to wait anymore.
            return false;
        }

        RingbufferContainer ringbuffer = getRingBufferContainer();
        if (ringbuffer.shouldWait(sequence)) {
            return true;
        }

        sequence = ringbuffer.readMany(sequence, resultSet);
        return !resultSet.isMinSizeReached();
    }

    @Override
    public void run() throws Exception {
        // no-op; we already did the work in the shouldWait method.
    }

    @Override
    public Object getResponse() {
        if (returnPortable) {
            List<Data> items = new ArrayList<Data>(resultSet.size());
            for (Data data : resultSet.getItems()) {
                items.add(data);
            }

            PortableReadResultSet portableReadResultSet = new PortableReadResultSet(resultSet.readCount(), items);
            return getNodeEngine().toData(portableReadResultSet);
        }

        return resultSet;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        RingbufferContainer ringbuffer = getRingBufferContainer();
        return ringbuffer.getRingEmptyWaitNotifyKey();
    }

    @Override
    public void onWaitExpire() {
        //todo:
    }

    @Override
    public int getId() {
        return READ_MANY_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(startSequence);
        out.writeInt(minSize);
        out.writeInt(maxSize);
        out.writeObject(filter);
        out.writeBoolean(returnPortable);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        startSequence = in.readLong();
        minSize = in.readInt();
        maxSize = in.readInt();
        filter = in.readObject();
        returnPortable = in.readBoolean();
    }
}
