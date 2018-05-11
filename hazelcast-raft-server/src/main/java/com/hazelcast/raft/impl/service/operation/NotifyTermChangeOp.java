/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.impl.service.operation;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.impl.service.TermChangeAwareService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public class NotifyTermChangeOp extends RaftOp implements IdentifiedDataSerializable {

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        ILogger logger = getLogger();
        for (TermChangeAwareService service : getNodeEngine().getServices(TermChangeAwareService.class)) {
            try {
                service.onNewTermCommit(groupId);
            } catch (Exception e) {
                logger.severe("onNewTermCommit() failed for service: " + service.getClass().getSimpleName()
                        + " and raft group: " + groupId, e);
            }
        }

        return null;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.NOTIFY_TERM_CHANGE_OP;
    }

    @Override
    protected String getServiceName() {
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }
}
