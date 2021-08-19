/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.exec;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.sql.impl.NodeServiceProvider;
import com.hazelcast.sql.impl.exec.io.InboundHandler;
import com.hazelcast.sql.impl.exec.io.OutboundHandler;
import com.hazelcast.sql.impl.exec.io.flowcontrol.FlowControl;
import com.hazelcast.sql.impl.exec.io.flowcontrol.FlowControlFactory;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.operation.QueryExecuteOperationFragment;
import com.hazelcast.sql.impl.operation.QueryExecuteOperationFragmentMapping;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Visitor which builds an executor for every observed physical node.
 * TODO: [sasha] intended to be removed.
 */
public class CreateExecPlanNodeVisitor {
    /**
     * Operation handler.
     */
    private final QueryOperationHandler operationHandler;

    /**
     * Node service provider.
     */
    private final NodeServiceProvider nodeServiceProvider;

    /**
     * Serialization service.
     */
    private final InternalSerializationService serializationService;

    /**
     * Local member ID.
     */
    private final UUID localMemberId;

    /**
     * Operation.
     */
    private final QueryExecuteOperation operation;

    /**
     * Factory to create flow control objects.
     */
    private final FlowControlFactory flowControlFactory;

    /**
     * Partitions owned by this data node.
     */
    private final PartitionIdSet localParts;

    /**
     * Recommended outbox batch size in bytes.
     */
    private final int outboxBatchSize;

    /**
     * Hook to alter produced Exec (for testing purposes).
     */
    private final CreateExecPlanNodeVisitorHook hook;

    /**
     * Stack of elements to be merged.
     */
    private final ArrayList<Exec> stack = new ArrayList<>(1);

    /**
     * Result.
     */
    private Exec exec;

    /**
     * Inboxes.
     */
    private final Map<Integer, InboundHandler> inboxes = new HashMap<>();

    /**
     * Outboxes.
     */
    private final Map<Integer, Map<UUID, OutboundHandler>> outboxes = new HashMap<>();

    public CreateExecPlanNodeVisitor(
            QueryOperationHandler operationHandler,
            NodeServiceProvider nodeServiceProvider,
            InternalSerializationService serializationService,
            UUID localMemberId,
            QueryExecuteOperation operation,
            FlowControlFactory flowControlFactory,
            PartitionIdSet localParts,
            int outboxBatchSize,
            CreateExecPlanNodeVisitorHook hook
    ) {
        this.operationHandler = operationHandler;
        this.nodeServiceProvider = nodeServiceProvider;
        this.serializationService = serializationService;
        this.localMemberId = localMemberId;
        this.operation = operation;
        this.flowControlFactory = flowControlFactory;
        this.localParts = localParts;
        this.outboxBatchSize = outboxBatchSize;
        this.hook = hook;
    }

    public Exec getExec() {
        return exec;
    }

    /**
     * For testing only.
     */
    public void setExec(Exec exec) {
        this.exec = exec;
    }

    public Map<Integer, InboundHandler> getInboxes() {
        return inboxes;
    }

    public Map<Integer, Map<UUID, OutboundHandler>> getOutboxes() {
        return outboxes;
    }

    /**
     * Public for testing purposes only.
     */
    public Exec pop() {
        return stack.remove(stack.size() - 1);
    }

    /**
     * Public for testing purposes only.
     */
    public void push(Exec exec) {
        CreateExecPlanNodeVisitorHook hook0 = hook;

        if (hook0 != null) {
            exec = hook0.onExec(exec);
        }

        stack.add(exec);
    }

    private FlowControl createFlowControl(int edgeId) {
        long initialMemory = operation.getEdgeInitialMemoryMap().get(edgeId);

        return flowControlFactory.create(initialMemory);
    }

    private Collection<UUID> getFragmentMembers(QueryExecuteOperationFragment fragment) {
        if (fragment.getMapping() == QueryExecuteOperationFragmentMapping.EXPLICIT) {
            return fragment.getMemberIds();
        }

        assert fragment.getMapping() == QueryExecuteOperationFragmentMapping.DATA_MEMBERS;

        return operation.getPartitionMap().keySet();
    }
}
