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

import com.hazelcast.cluster.MemberInfo;
import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.ResponseHandlerFactory;
import com.hazelcast.util.FutureUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class FinalizeJoinOperation extends MemberInfoUpdateOperation implements JoinOperation {

    public static final int FINALIZE_JOIN_TIMEOUT_FACTOR = 5;
    public static final int FINALIZE_JOIN_MAX_TIMEOUT = 30;

    private PostJoinOperation postJoinOp;

    public FinalizeJoinOperation() {
    }

    public FinalizeJoinOperation(Collection<MemberInfo> members, PostJoinOperation postJoinOp, long masterTime) {
        super(members, masterTime, true);
        this.postJoinOp = postJoinOp;
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

        Collection<Future> calls = null;
        if (postJoinOperations != null && postJoinOperations.length > 0) {
            final Collection<MemberImpl> members = clusterService.getMemberList();
            calls = new ArrayList<Future>(members.size());
            for (MemberImpl member : members) {
                if (!member.localMember()) {
                    PostJoinOperation operation = new PostJoinOperation(postJoinOperations);
                    Future f = operationService.createInvocationBuilder(ClusterServiceImpl.SERVICE_NAME,
                            operation, member.getAddress()).setTryCount(10).setTryPauseMillis(100).invoke();
                    calls.add(f);
                }
            }
        }

        if (postJoinOp != null) {
            postJoinOp.setNodeEngine(nodeEngine);
            OperationAccessor.setCallerAddress(postJoinOp, getCallerAddress());
            OperationAccessor.setConnection(postJoinOp, getConnection());
            postJoinOp.setResponseHandler(ResponseHandlerFactory.createEmptyResponseHandler());
            operationService.runOperationOnCallingThread(postJoinOp);
        }

        if (calls != null) {
            int timeout = Math.min(calls.size() * FINALIZE_JOIN_TIMEOUT_FACTOR, FINALIZE_JOIN_MAX_TIMEOUT);
            FutureUtil.waitWithDeadline(calls, timeout, TimeUnit.SECONDS, new FinalizeJoinExceptionHandler());
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
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        boolean hasPJOp = in.readBoolean();
        if (hasPJOp) {
            postJoinOp = new PostJoinOperation();
            postJoinOp.readData(in);
        }
    }

    private class FinalizeJoinExceptionHandler implements FutureUtil.ExceptionHandler {
        @Override
        public void handleException(Throwable throwable) {
            if (throwable instanceof ExecutionException) {
                ILogger logger = getLogger();
                if (logger.isFinestEnabled()) {
                    logger.finest("Error while executing post-join operations -> "
                            + throwable.getClass().getSimpleName() + "[" + throwable.getMessage() + "]", throwable);
                }
            }
        }
    }
}

