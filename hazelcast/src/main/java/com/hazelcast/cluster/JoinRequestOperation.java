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

package com.hazelcast.cluster;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class JoinRequestOperation extends AbstractClusterOperation implements JoinOperation {

    private JoinRequest message;

    public JoinRequestOperation() {
        super();
    }

    public JoinRequestOperation(JoinRequest message) {
        this.message = message;
    }

    @Override
    public void run() {
        ClusterServiceImpl cm = getService();
        cm.handleJoinRequest(this);
    }

    public JoinRequest getMessage() {
        return message;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        message = new JoinRequest();
        message.readData(in);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        message.writeData(out);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("JoinRequestOperation");
        sb.append("{message=").append(message);
        sb.append('}');
        return sb.toString();
    }
}
