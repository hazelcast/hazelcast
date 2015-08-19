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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.core.Member;
import com.hazelcast.replicatedmap.impl.messages.ReplicationMessage;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.replicatedmap.impl.record.ReplicationPublisher;
import com.hazelcast.spi.AbstractOperation;

/**
 * This operation will process the replication events received from other nodes in partition threads.
 * This operation runs locally.
 */
public class ProcessReplicationMessageOperation extends AbstractOperation {

    private final ReplicatedRecordStore store;
    private final ReplicationMessage replicationMessage;

    public ProcessReplicationMessageOperation(ReplicatedRecordStore store, ReplicationMessage message) {
        this.store = store;
        this.replicationMessage = message;
    }

    @Override
    public void run() throws Exception {
        Member origin = replicationMessage.getOrigin();
        if (getNodeEngine().getLocalMember().equals(origin)) {
            return;
        }
        ReplicationPublisher replicationPublisher = store.getReplicationPublisher();
        replicationPublisher.queueUpdateMessage(replicationMessage);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }
}
