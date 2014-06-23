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

package com.hazelcast.replicatedmap.operation;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import com.hazelcast.replicatedmap.messages.ReplicationMessage;
import com.hazelcast.replicatedmap.record.AbstractReplicatedRecordStore;
import com.hazelcast.replicatedmap.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.record.ReplicationPublisher;
import com.hazelcast.replicatedmap.record.VectorClockTimestamp;
import edu.umd.cs.findbugs.annotations.SuppressWarnings;

import java.io.IOException;

/**
 * Class for requesting an initial chunk of data from another node to pre-provision newly joining members
 */
public class ReplicatedMapInitChunkOperation
        extends AbstractReplicatedMapOperation
        implements IdentifiedDataSerializable {

    private String name;
    private Member origin;
    private ReplicatedRecord[] replicatedRecords;
    private int recordCount;
    private boolean finalChunk;
    private boolean notYetReadyChooseSomeoneElse;

    ReplicatedMapInitChunkOperation() {
    }

    public ReplicatedMapInitChunkOperation(String name, Member origin) {
        this(name, origin, new ReplicatedRecord[0], 0, true);
        this.notYetReadyChooseSomeoneElse = true;
    }

    // Findbugs warning suppressed since the array is serialized anyways and is never about to be changed
    @SuppressWarnings("EI_EXPOSE_REP")
    public ReplicatedMapInitChunkOperation(String name, Member origin, ReplicatedRecord[] replicatedRecords, int recordCount,
                                           boolean finalChunk) {
        this.name = name;
        this.origin = origin;
        this.replicatedRecords = replicatedRecords;
        this.recordCount = recordCount;
        this.finalChunk = finalChunk;
    }

    public String getName() {
        return name;
    }

    @Override
    public void run()
            throws Exception {
        ReplicatedMapService replicatedMapService = getService();

        AbstractReplicatedRecordStore recordStorage;
        recordStorage = (AbstractReplicatedRecordStore) replicatedMapService.getReplicatedRecordStore(name, true);

        ReplicationPublisher replicationPublisher = recordStorage.getReplicationPublisher();

        if (notYetReadyChooseSomeoneElse) {
            replicationPublisher.retryWithDifferentReplicationNode(origin);
        } else {
            for (int i = 0; i < recordCount; i++) {
                ReplicatedRecord record = replicatedRecords[i];

                Object key = record.getKey();
                Object value = record.getValue();
                VectorClockTimestamp timestamp = record.getVectorClockTimestamp();
                int updateHash = record.getLatestUpdateHash();
                long ttlMillis = record.getTtlMillis();

                ReplicationMessage update = new ReplicationMessage(name, key, value, timestamp, origin, updateHash, ttlMillis);
                replicationPublisher.queueUpdateMessage(update);
            }
            if (finalChunk) {
                recordStorage.finalChunkReceived();
            }
        }
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.OP_INIT_CHUNK;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(name);
        origin.writeData(out);
        out.writeInt(recordCount);
        for (int i = 0; i < recordCount; i++) {
            replicatedRecords[i].writeData(out);
        }
        out.writeBoolean(finalChunk);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        name = in.readUTF();
        origin = new MemberImpl();
        origin.readData(in);
        recordCount = in.readInt();
        replicatedRecords = new ReplicatedRecord[recordCount];
        for (int i = 0; i < recordCount; i++) {
            ReplicatedRecord replicatedRecord = new ReplicatedRecord();
            replicatedRecord.readData(in);
            replicatedRecords[i] = replicatedRecord;
        }
        finalChunk = in.readBoolean();
    }
}
