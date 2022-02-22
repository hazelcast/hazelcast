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
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.EXPLICIT_SUSPICION;

/**
 * An operation which is sent by a member that wants to be explicitly suspected by the target.
 * This suspicion request is triggered by a cluster operation that is sent by target to requester
 * but target is not known by requester.
 *
 * @since 3.9
 */
public class ExplicitSuspicionOp extends AbstractClusterOperation {

    private MembersViewMetadata membersViewMetadata;

    public ExplicitSuspicionOp() {
    }

    public ExplicitSuspicionOp(MembersViewMetadata membersViewMetadata) {
        this.membersViewMetadata = membersViewMetadata;
    }

    @Override
    public void run() throws Exception {
        Address suspectedAddress = getCallerAddress();
        getLogger().info("Received suspicion request from: " + suspectedAddress);

        final ClusterServiceImpl clusterService = getService();
        clusterService.handleExplicitSuspicion(membersViewMetadata, suspectedAddress);
    }

    @Override
    public int getClassId() {
        return EXPLICIT_SUSPICION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(membersViewMetadata);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        membersViewMetadata = in.readObject();
    }

}
