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

import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.JoinRequest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class JoinRequestOp extends AbstractClusterOperation {

    private JoinRequest request;

    public JoinRequestOp() {
    }

    public JoinRequestOp(JoinRequest request) {
        this.request = request;
    }

    @Override
    public void run() {
        ClusterServiceImpl cm = getService();
        cm.getClusterJoinManager().handleJoinRequest(request, getConnection());
    }

    public JoinRequest getRequest() {
        return request;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        request = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(request);
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", message=").append(request);
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.JOIN_REQUEST_OP;
    }
}
