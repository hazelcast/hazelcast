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
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import com.hazelcast.replicatedmap.record.AbstractReplicatedRecordStore;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import java.io.IOException;

/**
 * The replicated map post join operation to execute on remote nodes
 */
public class ReplicatedMapPostJoinOperation
        extends AbstractReplicatedMapOperation
        implements IdentifiedDataSerializable {

    /**
     * Default size for replication chunks
     */
    public static final int DEFAULT_CHUNK_SIZE = 100;

    private MemberMapPair[] replicatedMaps;
    private int chunkSize;

    ReplicatedMapPostJoinOperation() {
    }

    public ReplicatedMapPostJoinOperation(MemberMapPair[] replicatedMaps, int chunkSize) {
        this.replicatedMaps = replicatedMaps;
        this.chunkSize = chunkSize;
    }

    @Override
    public void run()
            throws Exception {
        Member localMember = getNodeEngine().getLocalMember();
        ReplicatedMapService replicatedMapService = getService();
        for (MemberMapPair replicatedMap : replicatedMaps) {
            String mapName = replicatedMap.getName();
            if (localMember.equals(replicatedMap.getMember())) {
                AbstractReplicatedRecordStore recordStorage = (AbstractReplicatedRecordStore) replicatedMapService
                        .getReplicatedRecordStore(mapName, false);

                if (recordStorage != null && recordStorage.isLoaded()) {
                    recordStorage.queueInitialFillup(getCallerAddress(), chunkSize);
                } else {
                    OperationService operationService = getNodeEngine().getOperationService();
                    Operation operation = new ReplicatedMapInitChunkOperation(mapName, localMember);
                    operationService.send(operation, getCallerAddress());
                }
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        out.writeInt(chunkSize);
        out.writeInt(replicatedMaps.length);
        for (int i = 0; i < replicatedMaps.length; i++) {
            replicatedMaps[i].writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        chunkSize = in.readInt();
        int length = in.readInt();
        replicatedMaps = new MemberMapPair[length];
        for (int i = 0; i < length; i++) {
            replicatedMaps[i] = new MemberMapPair();
            replicatedMaps[i].readData(in);
        }
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.OP_POST_JOIN;
    }

    /**
     * A mapping for replicated map names and the assigned provisioning member
     */
    public static class MemberMapPair
            implements DataSerializable {

        private Member member;
        private String name;

        MemberMapPair() {
        }

        public MemberMapPair(Member member, String name) {
            this.member = member;
            this.name = name;
        }

        public Member getMember() {
            return member;
        }

        public String getName() {
            return name;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {
            out.writeUTF(name);
            member.writeData(out);
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {
            name = in.readUTF();
            member = new MemberImpl();
            member.readData(in);
        }
    }

}
