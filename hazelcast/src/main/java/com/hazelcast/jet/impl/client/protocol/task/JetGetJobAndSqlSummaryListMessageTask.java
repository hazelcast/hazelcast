/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.client.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetGetJobAndSqlSummaryListCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMultiTargetMessageTask;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobAndSqlSummary;
import com.hazelcast.jet.impl.operation.GetJobAndSqlSummaryListOperation;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.JobPermission;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.internal.util.ExceptionUtil.rethrowFromCollection;
import static com.hazelcast.jet.impl.util.Util.checkJetIsEnabled;
import static com.hazelcast.jet.impl.util.Util.distinctBy;

public class JetGetJobAndSqlSummaryListMessageTask extends AbstractMultiTargetMessageTask<Void> {
    private static final Class[] IGNORED_EXCEPTIONS = {TargetNotMemberException.class, MemberLeftException.class};

    JetGetJobAndSqlSummaryListMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Supplier<Operation> createOperationSupplier() {
        return GetJobAndSqlSummaryListOperation::new;
    }

    @Override
    public Collection<Member> getTargets() {
        checkJetIsEnabled(nodeEngine);
        return nodeEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object reduce(Map<Member, Object> map) throws Throwable {
        rethrowFromCollection(map.values(), IGNORED_EXCEPTIONS);
        return map.values().stream()
                .filter(item -> !(item instanceof Throwable))
                .flatMap(item -> ((List<JobAndSqlSummary>) item).stream())
                // In edge cases there can be duplicates. E.g. the GetIdsOp is broadcast to all members.  member1
                // is master and responds and dies. It's removed from cluster, member2 becomes master and
                // responds with the same normal jobs. It's safe to remove duplicates because the same jobId should
                // be the same job - we use FlakeIdGenerator to generate the IDs.
                .filter(distinctBy(JobAndSqlSummary::getJobId))
                .collect(Collectors.toList());
    }

    @Override
    protected Void decodeClientMessage(ClientMessage clientMessage) {
        return null;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return JetGetJobAndSqlSummaryListCodec.encodeResponse((List<JobAndSqlSummary>) response);
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "getJobAndSqlSummaryList";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Override
    public String getServiceName() {
        return JetServiceBackend.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new JobPermission(ActionConstants.ACTION_READ);
    }
}
