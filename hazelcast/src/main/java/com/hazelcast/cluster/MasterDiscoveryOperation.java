/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

public class MasterDiscoveryOperation extends AbstractClusterOperation implements JoinOperation {

    private JoinMessage joinMessage;

    public MasterDiscoveryOperation() {
    }

    public MasterDiscoveryOperation(JoinMessage joinMessage) {
        this.joinMessage = joinMessage;
    }

    @Override
    public void run() {
        ClusterServiceImpl cm = getService();
        cm.answerMasterQuestion(joinMessage);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        joinMessage = new JoinMessage();
        joinMessage.readData(in);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        joinMessage.writeData(out);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MasterDiscoveryOperation");
        sb.append("{message=").append(joinMessage);
        sb.append('}');
        return sb.toString();
    }
}
