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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.WaitNotifyKey;

import java.io.IOException;

/**
 * Used to notify {@link AwaitMapFlushOperation} when {@link com.hazelcast.map.impl.mapstore.writebehind.StoreWorker StoreWorker}
 * managed to flush this {@link AwaitMapFlushOperation#flushSequence flushSequence}.
 *
 * @see AwaitMapFlushOperation
 */
public class NotifyMapFlushOperation extends MapOperation implements PartitionAwareOperation, ReadonlyOperation, Notifier {

    private long sequence;

    public NotifyMapFlushOperation(String name, long sequence) {
        super(name);
        this.sequence = sequence;
    }

    public NotifyMapFlushOperation() {
    }

    @Override
    public void run() throws Exception {
        // NOP.
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return new MapFlushWaitNotifyKey(name, getPartitionId(), sequence);
    }

    @Override
    public boolean shouldNotify() {
        return Boolean.TRUE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(sequence);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        sequence = in.readLong();
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.NOTIFY_MAP_FLUSH;
    }
}
