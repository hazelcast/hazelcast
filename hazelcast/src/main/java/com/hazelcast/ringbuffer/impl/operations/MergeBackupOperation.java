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

package com.hazelcast.ringbuffer.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.ringbuffer.impl.ArrayRingbuffer;
import com.hazelcast.ringbuffer.impl.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;

import java.io.IOException;

import static com.hazelcast.internal.nio.IOUtil.readObject;
import static com.hazelcast.internal.nio.IOUtil.writeObject;
import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.MERGE_BACKUP_OPERATION;

/**
 * Contains the entire ringbuffer as a result of split-brain healing with a
 * {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class MergeBackupOperation extends AbstractRingBufferOperation implements BackupOperation {

    private Ringbuffer<Object> ringbuffer;

    public MergeBackupOperation() {
    }

    MergeBackupOperation(String name, Ringbuffer<Object> ringbuffer) {
        super(name);
        this.ringbuffer = ringbuffer;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() throws Exception {
        final RingbufferService service = getService();
        if (ringbuffer == null) {
            service.destroyDistributedObject(name);
        } else {
            final RingbufferContainer existingContainer = getRingBufferContainer();
            existingContainer.setHeadSequence(ringbuffer.headSequence());
            existingContainer.setTailSequence(ringbuffer.tailSequence());

            for (long seq = ringbuffer.headSequence(); seq <= ringbuffer.tailSequence(); seq++) {
                existingContainer.set(seq, ringbuffer.read(seq));
            }
        }
    }

    @Override
    public int getClassId() {
        return MERGE_BACKUP_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(ringbuffer != null ? (int) ringbuffer.getCapacity() : 0);
        if (ringbuffer != null) {
            out.writeLong(ringbuffer.tailSequence());
            out.writeLong(ringbuffer.headSequence());
            for (Object item : ringbuffer) {
                writeObject(out, item);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int capacity = in.readInt();
        if (capacity > 0) {
            final long tailSequence = in.readLong();
            final long headSequence = in.readLong();
            ringbuffer = new ArrayRingbuffer<Object>(capacity);
            ringbuffer.setTailSequence(tailSequence);
            ringbuffer.setHeadSequence(headSequence);

            for (long seq = headSequence; seq <= tailSequence; seq++) {
                ringbuffer.set(seq, readObject(in));
            }
        }
    }
}
