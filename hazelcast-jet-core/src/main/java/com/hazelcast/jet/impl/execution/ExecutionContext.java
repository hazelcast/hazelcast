/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.function.Consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import static com.hazelcast.jet.impl.util.Util.getRemoteMembers;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

public class ExecutionContext {

    // vertex id --> ordinal --> receiver tasklet
    private Map<Integer, Map<Integer, ReceiverTasklet>> receiverMap = emptyMap();

    // dest vertex id --> dest ordinal --> dest addr --> sender tasklet
    private Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> senderMap = emptyMap();

    private List<ProcessorSupplier> procSuppliers = emptyList();
    private Map<Address, Integer> memberToId = emptyMap();
    private List<Tasklet> tasklets;
    private CompletionStage<Void> executionCompletionStage;

    private final long executionId;
    private final NodeEngine nodeEngine;
    private final ExecutionService execService;

    public ExecutionContext(long executionId, NodeEngine nodeEngine, ExecutionService execService) {
        this.executionId = executionId;
        this.execService = execService;
        this.nodeEngine = nodeEngine;
    }

    public CompletionStage<Void> execute(Consumer<CompletionStage<Void>> doneCallback) {
        executionCompletionStage = execService.execute(tasklets, doneCallback);
        executionCompletionStage.whenComplete((r, e) -> tasklets.clear());
        return executionCompletionStage;
    }

    public CompletionStage<Void> getExecutionCompletionStage() {
        return executionCompletionStage;
    }

    public Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> senderMap() {
        return senderMap;
    }

    public Map<Integer, Map<Integer, ReceiverTasklet>> receiverMap() {
        return receiverMap;
    }

    public void complete(Throwable error) {
        procSuppliers.forEach(s -> s.complete(error));
    }

    public void handlePacket(int vertexId, int ordinal, Address sender, BufferObjectDataInput in) {
        receiverMap.get(vertexId)
                   .get(ordinal)
                   .receiveStreamPacket(in, memberToId.get(sender));
    }

    public Integer getMemberId(Address member) {
        return memberToId != null ? memberToId.get(member) : null;
    }

    public ExecutionContext initialize(ExecutionPlan plan) {
        populateMemberToId();
        // Must be populated early, so all processor suppliers are
        // available to be completed in the case of init failure
        procSuppliers = unmodifiableList(plan.getProcessorSuppliers());
        plan.initialize(nodeEngine, executionId);
        receiverMap = unmodifiableMap(plan.getReceiverMap());
        senderMap = unmodifiableMap(plan.getSenderMap());
        tasklets = plan.getTasklets();
        return this;
    }

    private void populateMemberToId() {
        final Map<Address, Integer> memberToId = new HashMap<>();
        int id = 0;
        for (Address member : getRemoteMembers(nodeEngine)) {
            memberToId.put(member, id++);
        }
        this.memberToId = unmodifiableMap(memberToId);
    }

}
