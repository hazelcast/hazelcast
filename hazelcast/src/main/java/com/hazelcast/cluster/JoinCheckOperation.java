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

import com.hazelcast.instance.Node;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import java.io.IOException;

import static com.hazelcast.util.EmptyStatement.ignore;

/**
 * An operation that checks whether {@link com.hazelcast.cluster.JoinRequest} is valid or not.
 */
public class JoinCheckOperation extends AbstractOperation implements JoinOperation {

    private JoinRequest joinRequest;
    private JoinRequest response;

    public JoinCheckOperation() {
    }

    public JoinCheckOperation(final JoinRequest joinRequest) {
        this.joinRequest = joinRequest;
    }

    @Override
    public void run() {
        final ClusterServiceImpl service = getService();
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        final Node node = nodeEngine.getNode();
        boolean ok = false;
        if (joinRequest != null && node.joined() && node.isActive()) {
            try {
                ok = service.validateJoinMessage(joinRequest);
            } catch (Exception ignored) {
                ignore(ignored);
            }
        }
        if (ok) {
            response = node.createJoinRequest();
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
    protected void readInternal(final ObjectDataInput in) throws IOException {
        joinRequest = new JoinRequest();
        joinRequest.readData(in);
    }

    @Override
    protected void writeInternal(final ObjectDataOutput out) throws IOException {
        joinRequest.writeData(out);
    }
}

