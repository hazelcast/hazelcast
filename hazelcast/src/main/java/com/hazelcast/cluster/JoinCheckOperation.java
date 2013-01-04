/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.Node;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;

/**
 * @mdogan 8/2/12
 */
public class JoinCheckOperation extends AbstractOperation implements JoinOperation {

    private JoinInfo joinInfo;

    private transient JoinInfo response;

    public JoinCheckOperation() {
    }

    public JoinCheckOperation(final JoinInfo joinInfo) {
        this.joinInfo = joinInfo;
    }

    public void run() {
        final ClusterService service = getService();
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        final Node node = nodeEngine.getNode();
        boolean ok = false;
        if (joinInfo != null && node.joined() && node.isActive()) {
            try {
                ok = service.validateJoinRequest(joinInfo);
            } catch (Exception ignored) {
            }
        }
        if (ok) {
            response = node.createJoinInfo();
        }
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public void readInternal(final ObjectDataInput in) throws IOException {
        joinInfo = new JoinInfo();
        joinInfo.readData(in);
    }

    @Override
    public void writeInternal(final ObjectDataOutput out) throws IOException {
        joinInfo.writeData(out);
    }
}

