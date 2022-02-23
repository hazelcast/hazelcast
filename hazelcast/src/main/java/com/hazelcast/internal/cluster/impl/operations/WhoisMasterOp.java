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
import com.hazelcast.internal.cluster.impl.JoinMessage;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class WhoisMasterOp extends AbstractClusterOperation {

    private JoinMessage joinMessage;

    public WhoisMasterOp() {
    }

    public WhoisMasterOp(JoinMessage joinMessage) {
        this.joinMessage = joinMessage;
    }

    @Override
    public void run() {
        ClusterServiceImpl cm = getService();
        cm.getClusterJoinManager().answerWhoisMasterQuestion(joinMessage, getConnection());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        joinMessage = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(joinMessage);
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", message=").append(joinMessage);
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.WHOIS_MASTER;
    }
}
