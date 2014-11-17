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

package com.hazelcast.cluster.impl.operations;

import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.cluster.impl.JoinRequest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class JoinRequestOperation extends AbstractClusterOperation implements JoinOperation {

    private JoinRequest request;

    public JoinRequestOperation() {
    }

    public JoinRequestOperation(JoinRequest request) {
        this.request = request;
    }

    @Override
    public void run() {
        ClusterServiceImpl cm = getService();
        cm.handleJoinRequest(request, getConnection());
    }

    public JoinRequest getRequest() {
        return request;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        request = new JoinRequest();
        request.readData(in);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        request.writeData(out);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("JoinRequestOperation");
        sb.append("{message=").append(request);
        sb.append('}');
        return sb.toString();
    }
}
