/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;

public class BeforeJoinCheckFailureOperation extends AbstractClusterOperation
        implements JoinOperation {

    private String failReasonMsg;

    public BeforeJoinCheckFailureOperation() {
    }

    public BeforeJoinCheckFailureOperation(String failReasonMsg) {
        this.failReasonMsg = failReasonMsg;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(failReasonMsg);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        failReasonMsg = in.readUTF();
    }

    @Override
    public void run() {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        final Node node = nodeEngine.getNode();
        final ILogger logger = nodeEngine.getLogger("com.hazelcast.security");
        logger.severe("Node could not join cluster. Before join check failed node is going to shutdown now!");
        logger.severe("Reason of failure for node join : " + failReasonMsg);
        node.shutdown(true);
    }
}
