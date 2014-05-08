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

package com.hazelcast.replicatedmap.messages;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.operation.ReplicatedMapDataSerializerHook;
import edu.umd.cs.findbugs.annotations.SuppressWarnings;

import java.io.IOException;

/**
 * This replicated message contains multiple replications at once
 */
public class MultiReplicationMessage
        implements IdentifiedDataSerializable {

    private String name;
    private ReplicationMessage[] replicationMessages;

    public MultiReplicationMessage() {
    }

    // Findbugs warning suppressed since the array is serialized anyways and is never about to be changed
    @SuppressWarnings("EI_EXPOSE_REP")
    public MultiReplicationMessage(String name, ReplicationMessage[] replicationMessages) {
        this.name = name;
        this.replicationMessages = replicationMessages;
    }

    // Findbugs warning suppressed since the array is serialized anyways and is never about to be changed
    @SuppressWarnings("EI_EXPOSE_REP")
    public ReplicationMessage[] getReplicationMessages() {
        return replicationMessages;
    }

    public String getName() {
        return name;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(name);
        out.writeInt(replicationMessages.length);
        for (int i = 0; i < replicationMessages.length; i++) {
            replicationMessages[i].writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        name = in.readUTF();
        int length = in.readInt();
        replicationMessages = new ReplicationMessage[length];
        for (int i = 0; i < length; i++) {
            ReplicationMessage replicationMessage = new ReplicationMessage();
            replicationMessage.readData(in);
            replicationMessages[i] = replicationMessage;
        }
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.REPL_MULTI_UPDATE_MESSAGE;
    }

}
