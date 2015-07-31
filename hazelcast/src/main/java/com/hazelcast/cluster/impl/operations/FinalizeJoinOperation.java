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

import com.hazelcast.cluster.MemberInfo;
import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.core.Member;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createEmptyResponseHandler;

public class FinalizeJoinOperation extends MemberInfoUpdateOperation implements JoinOperation {

    public static final int FINALIZE_JOIN_TIMEOUT_FACTOR = 5;
    public static final int FINALIZE_JOIN_MAX_TIMEOUT = 60;

    private PostJoinOperation postJoinOp;
    private String clusterId;
    private long clusterStartTime;

    public FinalizeJoinOperation() {
    }

    public FinalizeJoinOperation(Collection<MemberInfo> members, PostJoinOperation postJoinOp, long masterTime) {
        super(members, masterTime, true);
        this.postJoinOp = postJoinOp;
    }

    public FinalizeJoinOperation(Collection<MemberInfo> members, PostJoinOperation postJoinOp,
                                 long masterTime, String clusterId, long clusterStartTime) {
        super(members, masterTime, true);
        this.postJoinOp = postJoinOp;
        this.clusterId = clusterId;
        this.clusterStartTime  = clusterStartTime;
    }

    public FinalizeJoinOperation(Collection<MemberInfo> members, PostJoinOperation postJoinOp,
                                 long masterTime, boolean sendResponse) {
        super(members, masterTime, sendResponse);
        this.postJoinOp = postJoinOp;
    }

    @Override
    public void run() throws Exception {
        if (!isValid()) {
            return;
        }

        processMemberUpdate();

        // Post join operations must be lock free; means no locks at all;
        // no partition locks, no key-based locks, no service level locks!

        final ClusterServiceImpl clusterService = getService();
        final NodeEngineImpl nodeEngine = clusterService.getNodeEngine();
        final Operation[] postJoinOperations = nodeEngine.getPostJoinOperations();
        final OperationService operationService = nodeEngine.getOperationService();

        clusterService.setClusterId(clusterId);
        clusterService.getClusterClock().setClusterStartTime(clusterStartTime);

        if (postJoinOperations != null && postJoinOperations.length > 0) {
            final Collection<Member> members = clusterService.getMembers();
            for (Member member : members) {
                if (!member.localMember()) {
                    PostJoinOperation operation = new PostJoinOperation(postJoinOperations);
                    operationService.createInvocationBuilder(ClusterServiceImpl.SERVICE_NAME,
                            operation, member.getAddress()).setTryCount(100).invoke();
                }
            }
        }

        if (postJoinOp != null) {
            postJoinOp.setNodeEngine(nodeEngine);
            OperationAccessor.setCallerAddress(postJoinOp, getCallerAddress());
            OperationAccessor.setConnection(postJoinOp, getConnection());
            postJoinOp.setOperationResponseHandler(createEmptyResponseHandler());
            operationService.runOperationOnCallingThread(postJoinOp);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        boolean hasPJOp = postJoinOp != null;
        out.writeBoolean(hasPJOp);
        if (hasPJOp) {
            postJoinOp.writeData(out);
        }
        out.writeUTF(clusterId);
        out.writeLong(clusterStartTime);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        boolean hasPJOp = in.readBoolean();
        if (hasPJOp) {
            postJoinOp = new PostJoinOperation();
            postJoinOp.readData(in);
        }
        clusterId = in.readUTF();
        clusterStartTime = in.readLong();
    }

    @Override
    public String toString() {
        return "FinalizeJoinOperation{"
                + "postJoinOp=" + postJoinOp
                + "} "
                + super.toString();
    }
}

