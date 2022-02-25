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

package com.hazelcast.cp.internal.raftop;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.TermChangeAwareService;

import java.io.IOException;

/**
 * When leader of a Raft group changes, this operation is automatically
 * committed to the Raft group so that related services can perform some
 * actions.
 */
public class NotifyTermChangeOp extends RaftOp implements IdentifiedDataSerializable {

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        ILogger logger = getLogger();
        for (TermChangeAwareService service : getNodeEngine().getServices(TermChangeAwareService.class)) {
            try {
                service.onNewTermCommit(groupId, commitIndex);
            } catch (Exception e) {
                logger.severe("onNewTermCommit() failed for service: " + service.getClass().getSimpleName()
                        + " and CP group: " + groupId, e);
            }
        }

        return null;
    }

    @Override
    protected String getServiceName() {
        return null;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.NOTIFY_TERM_CHANGE_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }
}
