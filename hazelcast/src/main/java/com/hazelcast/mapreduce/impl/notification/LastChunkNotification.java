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

package com.hazelcast.mapreduce.impl.notification;

import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This notification is used to notify the reducer that this chunk is the last chunk of the
 * defined partitionId.
 *
 * @param <KeyOut> type of the key
 * @param <Value>  type of the value
 */
public class LastChunkNotification<KeyOut, Value>
        extends MemberAwareMapReduceNotification {

    private Map<KeyOut, Value> chunk;
    private int partitionId;
    private Address sender;

    public LastChunkNotification() {
    }

    public LastChunkNotification(Address address, String name, String jobId, Address sender, int partitionId,
                                 Map<KeyOut, Value> chunk) {
        super(address, name, jobId);
        this.partitionId = partitionId;
        this.sender = sender;
        this.chunk = chunk;
    }

    public Map<KeyOut, Value> getChunk() {
        return chunk;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public Address getSender() {
        return sender;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        out.writeInt(chunk.size());
        for (Map.Entry<KeyOut, Value> entry : chunk.entrySet()) {
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }
        out.writeInt(partitionId);
        out.writeObject(sender);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        int size = in.readInt();
        chunk = new HashMap<KeyOut, Value>();
        for (int i = 0; i < size; i++) {
            KeyOut key = in.readObject();
            Value value = in.readObject();
            chunk.put(key, value);
        }
        partitionId = in.readInt();
        sender = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.REDUCER_LAST_CHUNK_MESSAGE;
    }

    @Override
    public String toString() {
        return "LastChunkNotification{" + "chunk=" + chunk + ", partitionId=" + partitionId + ", sender=" + sender + '}';
    }

}
