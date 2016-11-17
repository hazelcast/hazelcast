/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cascading.planner;

import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.management.state.ClientState;
import cascading.stats.FlowNodeStats;
import cascading.stats.FlowStepStats;
import com.hazelcast.jet.cascading.JetFlowProcess;
import com.hazelcast.jet2.DAG;
import com.hazelcast.jet2.JetEngine;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.jet2.Job;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class JetFlowStepJob extends FlowStepJob<JetEngineConfig> {
    public static final int POLLING_INTERVAL = 100;
    public static final int BLOCK_FOR_COMPLETED_CHILD_DETAIL_DURATION = 10000;
    public static final int STATS_STORE_INTERVAL = 10000;
    public static final String CASCADING_ENGINE_NAME = "cascading";
    private final JetFlowProcess process;
    private final DAG dag;
    private Future<Void> future;

    public JetFlowStepJob(JetFlowProcess process, DAG dag, ClientState clientState,
                          JetEngineConfig config,
                          BaseFlowStep<JetEngineConfig> flowStep) {
        super(clientState, config, flowStep, POLLING_INTERVAL, STATS_STORE_INTERVAL,
                BLOCK_FOR_COMPLETED_CHILD_DETAIL_DURATION);
        this.process = process;
        this.dag = dag;
    }

    @Override
    protected FlowStepStats createStepStats(ClientState clientState) {
        return new JetFlowStepStats(flowStep, clientState);
    }

    @Override
    protected void internalBlockOnStop() throws IOException {
    }

    @Override
    protected boolean isRemoteExecution() {
        return true;
    }

    @Override
    protected String internalJobId() {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    protected boolean internalNonBlockingIsSuccessful() throws IOException {
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            throwable = e.getCause();
        }
        return throwable == null;
    }

    @Override
    protected Throwable getThrowable() {
        return throwable;
    }

    @Override
    protected void internalNonBlockingStart() throws IOException {
        JetEngine jetEngine = JetEngine.get(process.getHazelcastInstance(), CASCADING_ENGINE_NAME, getConfig());
        Job job = jetEngine.newJob(dag);
        future = job.execute();
    }

    @Override
    protected void updateNodeStatus(FlowNodeStats flowNodeStats) {

    }

    @Override
    protected boolean internalNonBlockingIsComplete() throws IOException {
        return future.isDone();
    }

    @Override
    protected void dumpDebugInfo() {

    }

    @Override
    protected boolean internalIsStartedRunning() {
        return true;
    }

    @Override
    protected void internalCleanup() {

    }
}
