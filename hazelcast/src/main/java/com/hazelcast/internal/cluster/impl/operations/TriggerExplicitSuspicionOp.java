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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersViewMetadata;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.TRIGGER_EXPLICIT_SUSPICION;

/**
 * An operation which is sent to trigger {@link ExplicitSuspicionOp} on target member.
 *
 * @since 3.9
 */
public class TriggerExplicitSuspicionOp extends AbstractClusterOperation {

    private int callerMemberListVersion;

    private MembersViewMetadata suspectedMembersViewMetadata;

    public TriggerExplicitSuspicionOp() {
    }

    public TriggerExplicitSuspicionOp(int callerMemberListVersion, MembersViewMetadata suspectedMembersViewMetadata) {
        this.callerMemberListVersion = callerMemberListVersion;
        this.suspectedMembersViewMetadata = suspectedMembersViewMetadata;
    }

    @Override
    public void run() throws Exception {
        ClusterServiceImpl clusterService = getService();
        clusterService.handleExplicitSuspicionTrigger(getCallerAddress(), callerMemberListVersion, suspectedMembersViewMetadata);
    }

    @Override
    public int getClassId() {
        return TRIGGER_EXPLICIT_SUSPICION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(callerMemberListVersion);
        out.writeObject(suspectedMembersViewMetadata);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        callerMemberListVersion = in.readInt();
        suspectedMembersViewMetadata = in.readObject();
    }

}
